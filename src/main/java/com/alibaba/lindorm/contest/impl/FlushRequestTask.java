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

import static java.nio.file.StandardOpenOption.WRITE;

public class FlushRequestTask extends Thread {
    private final BlockingQueue<List<WriteRequestWrapper>> flushRequestQueue = new ArrayBlockingQueue<>(100);

    private final FileChannel writeFileChanel;

    private final ByteBuffer writeByteBuffer;

    public FlushRequestTask(File dataPath) throws IOException {
        String absolutePath = dataPath.getAbsolutePath();
        String s = absolutePath + File.separator + "t.data";
        File f = new File(s);
        f.createNewFile();
        writeFileChanel = FileChannel.open(f.toPath(), WRITE);
        writeByteBuffer = ByteBuffer.allocateDirect(1024 * 1024);
        writeByteBuffer.position(0);
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
        List<IndexBlock> indexBlockList = new ArrayList<>();
        //保存KV数据
        for (WriteRequestWrapper writeRequestWrapper : writeRequestWrapperList) {
            WriteRequest writeRequest = writeRequestWrapper.getWriteRequest();
            String tableName = writeRequest.getTableName();
            Schema schema = writeRequestWrapper.getSchema();
            Collection<Row> rows = writeRequest.getRows();
            for (Row row : rows) {
                Map<String, ColumnValue> columns = row.getColumns();
                Vin vin = row.getVin();
                for (Map.Entry<String, ColumnValue> entity : columns.entrySet()) {
                    int position = writeByteBuffer.position();
                    KeyValue keyValue = resolveKey(entity.getKey(), entity.getValue(), row.getTimestamp(), vin, schema);
                    writeByteBuffer.putShort(keyValue.getKeyLength());
                    writeByteBuffer.putShort(keyValue.getRowKeyLength());
                    writeByteBuffer.put(keyValue.getRowKey());
                    writeByteBuffer.putShort(keyValue.getColumnNameLength());
                    writeByteBuffer.put(keyValue.getColumnName());
                    writeByteBuffer.putLong(keyValue.getTimestamp());
                    writeByteBuffer.put(keyValue.getValueType());
                    writeByteBuffer.putShort(keyValue.getValueLength());
                    if (keyValue.getColumnType().equals(ColumnValue.ColumnType.COLUMN_TYPE_STRING)) {
                        writeByteBuffer.put(keyValue.getByteBufferValue());
                    }
                    if (keyValue.getColumnType().equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)) {
                        writeByteBuffer.putInt(keyValue.getIntegerValue());
                    }
                    if (keyValue.getColumnType().equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
                        writeByteBuffer.putDouble(keyValue.getDoubleValue());
                    }
                    IndexBlock indexBlock = new IndexBlock();
                    indexBlock.setPosition(position);
                    byte[] tableNameBytes = tableName.getBytes();
                    indexBlock.setTableNameLength((short) tableNameBytes.length);
                    indexBlock.setTableName(tableNameBytes);
                    indexBlock.setRowKeyLength(keyValue.getRowKeyLength());
                    indexBlock.setRowKey(keyValue.getRowKey());
                    indexBlockList.add(indexBlock);
                }
            }
        }
        int position = writeByteBuffer.position();
        int size = 0;
        //保存索引信息
        for (IndexBlock indexBlock : indexBlockList) {
            writeByteBuffer.putInt(indexBlock.getPosition());
            writeByteBuffer.putShort(indexBlock.getTableNameLength());
            writeByteBuffer.put(indexBlock.getTableName());
            writeByteBuffer.putShort(indexBlock.getRowKeyLength());
            writeByteBuffer.put(indexBlock.getRowKey());
            size += indexBlock.getIndexBlockSize();
        }
        //保存块元数据信息
        writeByteBuffer.putInt(position);
        writeByteBuffer.putInt(size);

        writeFileChanel.write(writeByteBuffer);
        writeFileChanel.force(false);

        for (WriteRequestWrapper writeRequestWrapper : writeRequestWrapperList) {
            writeRequestWrapper.getLock().lock();
            writeRequestWrapper.getCondition().signal();
            writeRequestWrapper.getLock().unlock();
        }
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
