package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private void doWrite(List<WriteRequestWrapper> writeRequestWrapperList) throws IOException {
        System.out.println("dataWriteFileChanel position before: " + dataWriteFileChanel.position());
        System.out.println("indexWriteFileChanel position before: " + indexWriteFileChanel.position());

        //保存KV数据
        for (WriteRequestWrapper writeRequestWrapper : writeRequestWrapperList) {
            List<IndexBlock> indexBlockList = new ArrayList<>();

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
                    dataWriteByteBuffer.putShort(keyValue.getKeyLength());
                    dataWriteByteBuffer.putShort(keyValue.getRowKeyLength());
                    dataWriteByteBuffer.put(keyValue.getRowKey());
                    dataWriteByteBuffer.putShort(keyValue.getColumnNameLength());
                    dataWriteByteBuffer.put(keyValue.getColumnName());
                    dataWriteByteBuffer.putLong(keyValue.getTimestamp());
                    dataWriteByteBuffer.put(keyValue.getValueType());
                    dataWriteByteBuffer.putShort(keyValue.getValueLength());
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
                indexBlock.setOffset((short) (position + dataWriteFileChanel.position()));
                indexBlock.setDataSize((short) (p1 - position));
                byte[] tableNameBytes = tableName.getBytes();
                indexBlock.setTableNameLength((short) tableNameBytes.length);
                indexBlock.setTableName(tableNameBytes);
                indexBlock.setRowKeyLength((short) Vin.VIN_LENGTH);
                indexBlock.setRowKey(vin.getVin());
                indexBlockList.add(indexBlock);
            }
            //保存索引信息
            for (IndexBlock indexBlock : indexBlockList) {
                indexWriteByteBuffer.putShort(indexBlock.getIndexBlockLength());
                indexWriteByteBuffer.putInt(indexBlock.getOffset());
                indexWriteByteBuffer.putShort(indexBlock.getDataSize());
                indexWriteByteBuffer.putShort(indexBlock.getTableNameLength());
                indexWriteByteBuffer.put(indexBlock.getTableName());
                indexWriteByteBuffer.putShort(indexBlock.getRowKeyLength());
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

        System.out.println("dataWriteFileChanel position after: " + dataWriteFileChanel.position());
        System.out.println("indexWriteFileChanel position after: " + indexWriteFileChanel.position());

        for (WriteRequestWrapper writeRequestWrapper : writeRequestWrapperList) {
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

        short rowKeyLength = Vin.VIN_LENGTH;
        keyValue.setRowKeyLength(rowKeyLength);

        byte[] rowKey = vin.getVin();
        keyValue.setRowKey(rowKey);

        byte[] columnName = columnNameStr.getBytes();
        short columnNameLength = (short) columnName.length;
        keyValue.setColumnName(columnName);
        keyValue.setColumnNameLength(columnNameLength);

        keyValue.setTimestamp(timestamp);

        ColumnValue.ColumnType columnType = columnValue.getColumnType();
        keyValue.setColumnType(columnType);
        keyValue.setColumnValue(columnValue);

        return keyValue;
    }
}
