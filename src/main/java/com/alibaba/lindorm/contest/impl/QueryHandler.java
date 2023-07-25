package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.READ;

public class QueryHandler {
    private final File dpFile;

    public QueryHandler(File dpFile) {
        this.dpFile = dpFile;
    }

    public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        long start = System.currentTimeMillis();
        String tableName = pReadReq.getTableName();
        Collection<Vin> vinList = pReadReq.getVins();
        Set<String> requestedColumns = pReadReq.getRequestedColumns();
        ArrayList<Row> result = query(tableName, vinList, requestedColumns, -1, -1);
        long end = System.currentTimeMillis();
        System.out.println("----- executeLatestQuery time: " + (end - start));
        return result;
    }

    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        long start = System.currentTimeMillis();
        String tableName = trReadReq.getTableName();
        Vin vin = trReadReq.getVin();
        Set<String> requestedColumns = trReadReq.getRequestedFields();
        ArrayList<Row> result = query(tableName, Collections.singletonList(vin), requestedColumns, trReadReq.getTimeLowerBound(), trReadReq.getTimeUpperBound());
        long end = System.currentTimeMillis();
        System.out.println("----- executeTimeRangeQuery time: " + (end - start));
        return result;
    }

    private ArrayList<Row> query(String tableName, Collection<Vin> vinList, Set<String> requestedColumns, long timeLowerBound, long timeUpperBound) throws IOException {
        //获取当前表所有的索引信息
        List<IndexBlock> indexBlocks = IndexBufferHandler.getIndexBlocks(tableName);
        Map<String, Vin> vinNameMap = vinList.stream().collect(Collectors.toMap(i -> new String(i.getVin()), i -> i));
        List<Row> rowList = new ArrayList<>();
        for (IndexBlock indexBlock : indexBlocks) {
            String rowKeyName = new String(indexBlock.getRowKey());
            Map<String, ColumnValue> columns = new HashMap<>();
            if (vinNameMap.containsKey(rowKeyName)) {
                Vin vin = vinNameMap.get(rowKeyName);
                long t = indexBlock.getTimestamp();

                ByteBuffer sizeByteBuffer = ByteBuffer.allocateDirect(indexBlock.getDataSize());
                FileChannel fileChannel = FileChannel.open(dpFile.toPath(), READ);
                if (fileChannel.size() == 0) {
                    return new ArrayList<>();
                }
                fileChannel.read(sizeByteBuffer, indexBlock.getOffset());
                sizeByteBuffer.flip();
                int bufferPosition = sizeByteBuffer.position();
                int bufferLimit = sizeByteBuffer.limit();
                while (bufferPosition < bufferLimit) {
                    sizeByteBuffer.getInt();
                    int rowKeyLength = sizeByteBuffer.getInt();
                    byte[] rowKey = new byte[rowKeyLength];
                    for (int i = 0; i < rowKeyLength; i++) {
                        rowKey[i] = sizeByteBuffer.get();
                    }
                    String existRowKey = new String(rowKey);
                    int columnNameLength = sizeByteBuffer.getInt();
                    byte[] columnName = new byte[columnNameLength];
                    for (int i = 0; i < columnNameLength; i++) {
                        columnName[i] = sizeByteBuffer.get();
                    }
                    String existColumnName = new String(columnName);
                    long timestamp = sizeByteBuffer.getLong();
                    byte valueType = sizeByteBuffer.get();
                    int valueLength = sizeByteBuffer.getInt();
                    if (valueType == 1) {
                        byte[] valueBytes = new byte[valueLength];
                        for (int i = 0; i < valueLength; i++) {
                            valueBytes[i] = sizeByteBuffer.get();
                        }
                        if (requestedColumns.contains(existColumnName)) {
                            columns.put(existColumnName, new ColumnValue.StringColumn(ByteBuffer.wrap(valueBytes)));
                        }
                    }
                    if (valueType == 2) {
                        int value = sizeByteBuffer.getInt();
                        if (requestedColumns.contains(existColumnName)) {
                            columns.put(existColumnName, new ColumnValue.IntegerColumn(value));
                        }
                    }
                    if (valueType == 3) {
                        double value = sizeByteBuffer.getDouble();
                        if (requestedColumns.contains(existColumnName)) {
                            columns.put(existColumnName, new ColumnValue.DoubleFloatColumn(value));
                        }
                    }
                    bufferPosition = sizeByteBuffer.position();
                    bufferLimit = sizeByteBuffer.limit();
                }
                fileChannel.close();
                //构建Row
                Row row = new Row(vin, t, columns);
                rowList.add(row);
            }
        }

        ArrayList<Row> result = new ArrayList<>();
        Map<Vin, Row> resultMap = new HashMap<>();
        for (Row row : rowList) {
            //范围查询
            if (timeLowerBound != -1 && timeUpperBound != -1) {
                if (row.getTimestamp() >= timeLowerBound && row.getTimestamp() < timeUpperBound) {
                    result.add(row);
                }
            } else {
                if (!resultMap.containsKey(row.getVin()) || resultMap.get(row.getVin()).getTimestamp() < row.getTimestamp()) {
                    resultMap.put(row.getVin(), row);
                }
            }
        }
        if (resultMap.size() > 0) {
            return new ArrayList<>(resultMap.values());
        }
        return result;
    }
}
