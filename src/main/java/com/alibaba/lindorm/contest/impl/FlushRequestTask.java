package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardOpenOption.APPEND;

public class FlushRequestTask extends Thread {
    private final BlockingQueue<List<WriteRequestWrapper>> flushRequestQueue = new ArrayBlockingQueue<>(100);

    private final FileChannel dataWriteFileChanel;

    private final ByteBuffer dataWriteByteBuffer;

    private final FileChannel indexWriteFileChanel;

    private final ByteBuffer indexWriteByteBuffer;

    public FlushRequestTask(File dpFile, File ipFile) throws IOException {
        dataWriteFileChanel = FileChannel.open(dpFile.toPath(), APPEND);
        //100M
        dataWriteByteBuffer = ByteBuffer.allocateDirect(1024 * 1024 * 100);

        indexWriteFileChanel = FileChannel.open(ipFile.toPath(), APPEND);
        //100M
        indexWriteByteBuffer = ByteBuffer.allocateDirect(1024 * 1024 * 100);
    }

    public BlockingQueue<List<WriteRequestWrapper>> getFlushRequestQueue() {
        return flushRequestQueue;
    }

    @Override
    public void run() {
        try {
            while (true) {
                List<WriteRequestWrapper> writeRequestWrapperList = flushRequestQueue.poll(5, TimeUnit.MILLISECONDS);
                if (writeRequestWrapperList != null && writeRequestWrapperList.size() > 0) {
                    //执行刷盘操作
                    doWrite(writeRequestWrapperList);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        }
    }

    private void doWrite(List<WriteRequestWrapper> writeRequestWrapperList) throws IOException {
        //保存KV数据
        Iterator<WriteRequestWrapper> iterator = writeRequestWrapperList.iterator();
        while (iterator.hasNext()) {
            WriteRequestWrapper writeRequestWrapper = iterator.next();
            List<IndexBlock> indexBlockList = new CopyOnWriteArrayList<>();

            WriteRequest writeRequest = writeRequestWrapper.getWriteRequest();
            String tableName = writeRequest.getTableName();
            Schema schema = writeRequestWrapper.getSchema();
            Collection<Row> rows = writeRequest.getRows();
            for (Row row : rows) {
                Map<String, ColumnValue> columns = row.getColumns();
                Vin vin = row.getVin();
                int position = dataWriteByteBuffer.position();
                for (Map.Entry<String, ColumnValue> entity : columns.entrySet()) {
                    KeyValue keyValue = resolveKey(entity.getKey(), entity.getValue(), row.getTimestamp(), vin, schema);
                    dataWriteByteBuffer.putInt(keyValue.getKeyLength());
                    dataWriteByteBuffer.putInt(keyValue.getRowKeyLength());
                    dataWriteByteBuffer.put(keyValue.getRowKey());
                    dataWriteByteBuffer.putInt(keyValue.getColumnNameLength());
                    dataWriteByteBuffer.put(keyValue.getColumnName());
                    dataWriteByteBuffer.putLong(keyValue.getTimestamp());
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
                indexBlock.setOffset((int) (position + dataWriteFileChanel.position()));
                indexBlock.setDataSize(p1 - position);
                byte[] tableNameBytes = tableName.getBytes();
                indexBlock.setTableNameLength(tableNameBytes.length);
                indexBlock.setTableName(tableNameBytes);
                indexBlock.setRowKeyLength(Vin.VIN_LENGTH);
                indexBlock.setRowKey(vin.getVin());
                indexBlockList.add(indexBlock);
            }
            //保存索引信息
            for (IndexBlock indexBlock : indexBlockList) {
                indexWriteByteBuffer.putInt(indexBlock.getIndexBlockLength());
                indexWriteByteBuffer.putInt(indexBlock.getOffset());
                indexWriteByteBuffer.putInt(indexBlock.getDataSize());
                indexWriteByteBuffer.putInt(indexBlock.getTableNameLength());
                indexWriteByteBuffer.put(indexBlock.getTableName());
                indexWriteByteBuffer.putInt(indexBlock.getRowKeyLength());
                indexWriteByteBuffer.put(indexBlock.getRowKey());
            }
            IndexBufferHandler.offerIndex(tableName, indexBlockList);
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

    private ColumnValue.ColumnType getColumnType(String columnName, Schema schema) {
        Map<String, ColumnValue.ColumnType> columnTypeMap = schema.getColumnTypeMap();
        return columnTypeMap.get(columnName);
    }

    private KeyValue resolveKey(String columnNameStr, ColumnValue columnValue, long timestamp, Vin vin, Schema schema) {
        KeyValue keyValue = new KeyValue();

        int rowKeyLength = Vin.VIN_LENGTH;
        keyValue.setRowKeyLength(rowKeyLength);

        byte[] rowKey = vin.getVin();
        keyValue.setRowKey(rowKey);

        byte[] columnName = columnNameStr.getBytes();
        int columnNameLength = columnName.length;
        keyValue.setColumnName(columnName);
        keyValue.setColumnNameLength(columnNameLength);

        keyValue.setTimestamp(timestamp);

        ColumnValue.ColumnType columnType = columnValue.getColumnType();
        keyValue.setColumnType(columnType);
        keyValue.setColumnValue(columnValue);

        return keyValue;
    }
}
