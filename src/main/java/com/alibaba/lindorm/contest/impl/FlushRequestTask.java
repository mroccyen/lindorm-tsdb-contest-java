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
import java.util.concurrent.locks.Lock;

public class FlushRequestTask extends Thread {
    private final ByteBuffer dataWriteByteBuffer;
    private final FileManager fileManager;
    private boolean stop = false;
    private final HandleRequestTask handleRequestTask;

    public FlushRequestTask(FileManager fileManager, HandleRequestTask handleRequestTask) {
        this.handleRequestTask = handleRequestTask;
        this.fileManager = fileManager;
        //每个线程有10M缓冲区用于写数据
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
                Vin vin = row.getVin();
                //获得写文件锁
                //Lock writeLock = fileManager.getWriteLock(tableName, vin);
                //writeLock.lock();

                FileChannel dataWriteFileChanel = fileManager.getWriteFilChannel(tableName, vin);
                SchemaMeta schemaMeta = fileManager.getSchemaMeta(tableName);
                long position = dataWriteFileChanel.position();
                //vin has 17 byte
                dataWriteByteBuffer.put(vin.getVin());
                dataWriteByteBuffer.putLong(row.getTimestamp());
                for (int i = 0; i < schemaMeta.getColumnsNum(); ++i) {
                    String cName = schemaMeta.getColumnsName().get(i);
                    ColumnValue cVal = row.getColumns().get(cName);
                    switch (cVal.getColumnType()) {
                        case COLUMN_TYPE_STRING:
                            ColumnValue.StringColumn stringColumn = (ColumnValue.StringColumn) cVal;
                            dataWriteByteBuffer.putInt(stringColumn.getStringValue().remaining());
                            dataWriteByteBuffer.put(stringColumn.getStringValue());
                            break;
                        case COLUMN_TYPE_INTEGER:
                            ColumnValue.IntegerColumn integerColumn = (ColumnValue.IntegerColumn) cVal;
                            dataWriteByteBuffer.putInt(integerColumn.getIntegerValue());
                            break;
                        case COLUMN_TYPE_DOUBLE_FLOAT:
                            ColumnValue.DoubleFloatColumn doubleFloatColumn = (ColumnValue.DoubleFloatColumn) cVal;
                            dataWriteByteBuffer.putDouble(doubleFloatColumn.getDoubleFloatValue());
                            break;
                        default:
                            throw new IllegalStateException("Invalid column type");
                    }
                }
                Index index = new Index();
                index.setOffset(position);
                index.setRowKey(vin.getVin());

                //刷盘
                dataWriteByteBuffer.flip();
                dataWriteFileChanel.write(dataWriteByteBuffer);
                dataWriteFileChanel.force(false);

                dataWriteByteBuffer.clear();
                //add index
                IndexLoader.offerIndex(tableName, row.getTimestamp(), index);
                //释放写文件锁
                //writeLock.unlock();
            }
            //释放锁让写线程返回
            writeRequestWrapper.getLock().lock();
            writeRequestWrapper.getCondition().signal();
            writeRequestWrapper.getLock().unlock();
        }
    }
}
