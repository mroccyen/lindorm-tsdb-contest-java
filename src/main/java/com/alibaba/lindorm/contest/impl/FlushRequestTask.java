package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.structs.WriteRequest;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardOpenOption.APPEND;

public class FlushRequestTask extends Thread {
    private final Map<String, FileChannelPear> dataWriteFileChanelMap = new HashMap<>();

    private final ByteBuffer dataWriteByteBuffer;

    private final Map<String, FileChannelPear> indexWriteFileChanelMap = new HashMap<>();

    private final ByteBuffer indexWriteByteBuffer;

    private boolean stop = false;

    private final HandleRequestTask handleRequestTask;


    public FlushRequestTask(HandleRequestTask handleRequestTask) {
        //100M
        dataWriteByteBuffer = ByteBuffer.allocateDirect(1024 * 1024 * 100);
        //100M
        indexWriteByteBuffer = ByteBuffer.allocateDirect(1024 * 1024 * 100);

        this.handleRequestTask = handleRequestTask;
    }

    public void addFileChannel(String tableName, File dpFile, File ipFile, int index) throws IOException {
        FileChannel indexWriteFileChanel = FileChannel.open(ipFile.toPath(), APPEND);
        FileChannelPear indexFileChannelPear = new FileChannelPear();
        indexFileChannelPear.setIndex(index);
        indexFileChannelPear.setFileChannel(indexWriteFileChanel);
        indexWriteFileChanelMap.put(tableName, indexFileChannelPear);

        FileChannel dataWriteFileChanel = FileChannel.open(dpFile.toPath(), APPEND);
        FileChannelPear dataFileChannelPear = new FileChannelPear();
        dataFileChannelPear.setIndex(index);
        dataFileChannelPear.setFileChannel(dataWriteFileChanel);
        dataWriteFileChanelMap.put(tableName, dataFileChannelPear);
    }

    public void shutdown() {
        stop = true;
        try {
            for (Map.Entry<String, FileChannelPear> e : dataWriteFileChanelMap.entrySet()) {
                FileChannel fileChannel = e.getValue().getFileChannel();
                fileChannel.force(false);
                System.out.println(">>> shutdown file" + e.getValue().getIndex() + " exist data file size: " + fileChannel.size());
            }
            for (Map.Entry<String, FileChannelPear> e : indexWriteFileChanelMap.entrySet()) {
                FileChannel fileChannel = e.getValue().getFileChannel();
                fileChannel.force(false);
                System.out.println(">>> shutdown file" + e.getValue().getIndex() + " exist index file size: " + fileChannel.size());
            }
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
            FileChannelPear dataFileChannelPear = dataWriteFileChanelMap.get(tableName);
            FileChannel dataWriteFileChanel = dataFileChannelPear.getFileChannel();
            FileChannel indexWriteFileChanel = indexWriteFileChanelMap.get(tableName).getFileChannel();
            Collection<Row> rows = writeRequest.getRows();
            for (Row row : rows) {
                Map<String, ColumnValue> columns = row.getColumns();
                Vin vin = row.getVin();
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
                indexBlock.setIndex((byte) dataFileChannelPear.getIndex());
                indexBlock.setDataSize(p1 - position);
                indexBlock.setRowKey(vin.getVin());

                indexWriteByteBuffer.put(indexBlock.getIndexBlockLength());
                indexWriteByteBuffer.putLong(indexBlock.getOffset());
                indexWriteByteBuffer.putLong(indexBlock.getTimestamp());
                indexWriteByteBuffer.put((byte) dataFileChannelPear.getIndex());
                indexWriteByteBuffer.putInt(indexBlock.getDataSize());
                indexWriteByteBuffer.put(indexBlock.getRowKey());

                IndexBufferHandler.offerIndex(tableName, indexBlock);
            }

            dataWriteByteBuffer.flip();
            dataWriteFileChanel.write(dataWriteByteBuffer);
            dataWriteFileChanel.force(false);
            indexWriteByteBuffer.flip();
            indexWriteFileChanel.write(indexWriteByteBuffer);
            indexWriteFileChanel.force(false);

            dataWriteByteBuffer.clear();
            indexWriteByteBuffer.clear();
        }

        iterator = writeRequestWrapperList.iterator();
        while (iterator.hasNext()) {
            WriteRequestWrapper writeRequestWrapper = iterator.next();
            writeRequestWrapper.getLock().lock();
            writeRequestWrapper.getCondition().signal();
            writeRequestWrapper.getLock().unlock();
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

    public static class FileChannelPear {
        private int index;
        private FileChannel fileChannel;

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public FileChannel getFileChannel() {
            return fileChannel;
        }

        public void setFileChannel(FileChannel fileChannel) {
            this.fileChannel = fileChannel;
        }
    }
}
