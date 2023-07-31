package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.structs.WriteRequest;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardOpenOption.APPEND;

public class FlushRequestTask extends Thread {
    private final FileChannel dataWriteFileChanel;

    private final ByteBuffer dataWriteByteBuffer;

    private final FileChannel indexWriteFileChanel;

    private final ByteBuffer indexWriteByteBuffer;

    private boolean stop = false;

    private final HandleRequestTask handleRequestTask;

    private final byte index;

    public FlushRequestTask(HandleRequestTask handleRequestTask, File dpFile, File ipFile, byte index) throws IOException {
        dataWriteFileChanel = FileChannel.open(dpFile.toPath(), APPEND);
        //100M
        dataWriteByteBuffer = ByteBuffer.allocateDirect(1024 * 1024 * 100);

        indexWriteFileChanel = FileChannel.open(ipFile.toPath(), APPEND);
        //100M
        indexWriteByteBuffer = ByteBuffer.allocateDirect(1024 * 1024 * 100);

        this.handleRequestTask = handleRequestTask;
        this.index = index;
    }

    public void shutdown() {
        stop = true;
        try {
            dataWriteFileChanel.force(false);
            indexWriteFileChanel.force(false);
            System.out.println(">>> shutdown file" + index + " exist data file size: " + dataWriteFileChanel.size());
            System.out.println(">>> shutdown file" + index + " exist index file size: " + indexWriteFileChanel.size());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        }
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
                int position = dataWriteByteBuffer.position();
                for (Map.Entry<String, ColumnValue> entity : columns.entrySet()) {
                    KeyValue keyValue = resolveKey(entity.getKey(), entity.getValue(), row.getTimestamp(), vin);
                    dataWriteByteBuffer.put(keyValue.getRowKey());
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
                indexBlock.setIndex(index);
                indexBlock.setDataSize(p1 - position);
                byte[] tableNameBytes = tableName.getBytes();
                indexBlock.setTableNameLength((byte) tableNameBytes.length);
                indexBlock.setTableName(tableNameBytes);
                indexBlock.setRowKey(vin.getVin());

                indexWriteByteBuffer.put(indexBlock.getIndexBlockLength());
                indexWriteByteBuffer.putLong(indexBlock.getOffset());
                indexWriteByteBuffer.putLong(indexBlock.getTimestamp());
                indexWriteByteBuffer.put(index);
                indexWriteByteBuffer.putInt(indexBlock.getDataSize());
                indexWriteByteBuffer.put(indexBlock.getTableNameLength());
                indexWriteByteBuffer.put(indexBlock.getTableName());
                indexWriteByteBuffer.put(indexBlock.getRowKey());

                IndexBufferHandler.offerIndex(tableName, indexBlock);
            }
        }
        dataWriteByteBuffer.flip();
        dataWriteFileChanel.write(dataWriteByteBuffer);
        dataWriteFileChanel.force(false);
        indexWriteByteBuffer.flip();
        indexWriteFileChanel.write(indexWriteByteBuffer);
        indexWriteFileChanel.force(false);

        iterator = writeRequestWrapperList.iterator();
        while (iterator.hasNext()) {
            WriteRequestWrapper writeRequestWrapper = iterator.next();
            writeRequestWrapper.getLock().lock();
            writeRequestWrapper.getCondition().signal();
            writeRequestWrapper.getLock().unlock();
        }

        dataWriteByteBuffer.clear();
        indexWriteByteBuffer.clear();
    }

    private KeyValue resolveKey(String columnNameStr, ColumnValue columnValue, long timestamp, Vin vin) {
        KeyValue keyValue = new KeyValue();

        byte[] rowKey = vin.getVin();
        keyValue.setRowKey(rowKey);

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
