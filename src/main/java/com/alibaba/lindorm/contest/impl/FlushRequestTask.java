package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.structs.WriteRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class FlushRequestTask extends Thread {
    private final ByteBuffer dataWriteByteBuffer;
    private final FileManager fileManager;
    private boolean stop = false;
    private final HandleRequestTask handleRequestTask;

    public FlushRequestTask(FileManager fileManager, HandleRequestTask handleRequestTask) {
        this.handleRequestTask = handleRequestTask;
        this.fileManager = fileManager;
        //10M
        dataWriteByteBuffer = ByteBuffer.allocateDirect(1024 * 1024 * 10);
    }

    public void shutdown() {
        stop = true;
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                List<WriteRequestWrapper> writeRequestWrapperList = handleRequestTask.getFlushRequestQueue().poll(5, TimeUnit.MILLISECONDS);
                if (writeRequestWrapperList != null && writeRequestWrapperList.size() > 0) {
                    //执行刷盘操作
                    doWrite(writeRequestWrapperList);
                }
            } catch (Exception e) {
                System.out.println(">>> " + Thread.currentThread().getName() + " FlushRequestTask happen exception: " + e.getMessage());
                for (StackTraceElement stackTraceElement : e.getStackTrace()) {
                    System.out.println(">>> " + Thread.currentThread().getName() + " FlushRequestTask happen exception: " + stackTraceElement.toString());
                }
                System.exit(-1);
            }
        }
    }

    private void doWrite(List<WriteRequestWrapper> writeRequestWrapperList) throws IOException {
        //保存KV数据
        Iterator<WriteRequestWrapper> iterator = writeRequestWrapperList.iterator();
        while (iterator.hasNext()) {
            WriteRequestWrapper writeRequestWrapper = iterator.next();

            WriteRequest writeRequest = writeRequestWrapper.getWriteRequest();
            String tableName = writeRequest.getTableName();
            Collection<Row> rows = writeRequest.getRows();
            for (Row row : rows) {
                Map<String, ColumnValue> columns = row.getColumns();
                Vin vin = row.getVin();
                FileChannel dataWriteFileChanel = fileManager.getWriteFilChannel(tableName, vin);
                int position = dataWriteByteBuffer.position();
                for (Map.Entry<String, ColumnValue> entity : columns.entrySet()) {
                    KeyValue keyValue = resolveKey(entity.getKey(), entity.getValue());
                    dataWriteByteBuffer.put(keyValue.getColumnNameLength());
                    dataWriteByteBuffer.put(keyValue.getColumnName());
                    dataWriteByteBuffer.put(keyValue.getValueType());
                    dataWriteByteBuffer.putInt(keyValue.getValueLength());
                    if (keyValue.getColumnType().equals(ColumnValue.ColumnType.COLUMN_TYPE_STRING)) {
                        dataWriteByteBuffer.put(keyValue.getByteBufferValue());
                    }
                    if (keyValue.getColumnType().equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)) {
                        dataWriteByteBuffer.putInt(keyValue.getIntegerValue());
                    }
                    if (keyValue.getColumnType().equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
                        dataWriteByteBuffer.putDouble(keyValue.getDoubleValue());
                    }
                }
                int p1 = dataWriteByteBuffer.position();
                IndexBlock indexBlock = new IndexBlock();
                indexBlock.setOffset(position + dataWriteFileChanel.position());
                indexBlock.setTimestamp(row.getTimestamp());
                indexBlock.setDataSize(p1 - position);
                indexBlock.setRowKey(vin.getVin());

                //刷盘
                dataWriteByteBuffer.flip();
                dataWriteFileChanel.write(dataWriteByteBuffer);
                dataWriteFileChanel.force(false);

                dataWriteByteBuffer.clear();
                //add index
                IndexBufferLoader.offerIndex(tableName, indexBlock);
                //释放锁让写线程返回
                writeRequestWrapper.getLock().lock();
                writeRequestWrapper.getCondition().signal();
                writeRequestWrapper.getLock().unlock();
            }
        }
    }

    private KeyValue resolveKey(String columnNameStr, ColumnValue columnValue) {
        KeyValue keyValue = new KeyValue();

        byte[] columnName = columnNameStr.getBytes();
        int columnNameLength = columnName.length;
        keyValue.setColumnName(columnName);
        keyValue.setColumnNameLength((byte) columnNameLength);

        ColumnValue.ColumnType columnType = columnValue.getColumnType();
        keyValue.setColumnType(columnType);
        keyValue.setColumnValue(columnValue);

        return keyValue;
    }
}
