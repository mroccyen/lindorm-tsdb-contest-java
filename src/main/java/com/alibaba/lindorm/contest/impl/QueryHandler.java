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
        List<IndexBlock> indexBlocks = IndexBufferHandler.getIndexBlocks(tableName);
        Map<String, List<IndexBlock>> rowKeyMap = indexBlocks.stream().collect(Collectors.groupingBy(i -> new String(i.getRowKey())));
        Map<Vin, List<QueryResult>> vinMap = new HashMap<>();
        for (Vin vin : vinList) {
            List<QueryResult> queryResultList = new ArrayList<>();
            String rowKeyName = new String(vin.getVin());
            if (rowKeyMap.containsKey(rowKeyName)) {
                List<IndexBlock> indexBlockList = rowKeyMap.get(rowKeyName);
                for (IndexBlock indexBlock : indexBlockList) {
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
                            if (existRowKey.equals(rowKeyName) && requestedColumns.contains(existColumnName)) {
                                QueryResult queryResult = new QueryResult();
                                queryResult.setTimestamp(timestamp);
                                queryResult.setColumnName(existColumnName);
                                queryResult.setColumnValue(new ColumnValue.StringColumn(ByteBuffer.wrap(valueBytes)));
                                queryResultList.add(queryResult);
                            }
                        }
                        if (valueType == 2) {
                            int value = sizeByteBuffer.getInt();
                            if (existRowKey.equals(rowKeyName) && requestedColumns.contains(existColumnName)) {
                                QueryResult queryResult = new QueryResult();
                                queryResult.setTimestamp(timestamp);
                                queryResult.setColumnName(existColumnName);
                                queryResult.setColumnValue(new ColumnValue.IntegerColumn(value));
                                queryResultList.add(queryResult);
                            }
                        }
                        if (valueType == 3) {
                            double value = sizeByteBuffer.getDouble();
                            if (existRowKey.equals(rowKeyName) && requestedColumns.contains(existColumnName)) {
                                QueryResult queryResult = new QueryResult();
                                queryResult.setTimestamp(timestamp);
                                queryResult.setColumnName(existColumnName);
                                queryResult.setColumnValue(new ColumnValue.DoubleFloatColumn(value));
                                queryResultList.add(queryResult);
                            }
                        }
                        bufferPosition = sizeByteBuffer.position();
                        bufferLimit = sizeByteBuffer.limit();
                    }
                    fileChannel.close();
                }
            }
            vinMap.put(vin, queryResultList);
        }

        ArrayList<Row> result = new ArrayList<>();
        for (Map.Entry<Vin, List<QueryResult>> entity : vinMap.entrySet()) {
            List<QueryResult> value = entity.getValue();
            Map<String, List<QueryResult>> map = value.stream().collect(Collectors.groupingBy(QueryResult::getColumnName));
            //存储最新值查询的列值信息
            Map<String, ColumnValue> latestQueryColumns = new HashMap<>();
            //存储范围查询的列值信息
            Map<Long, Map<String, ColumnValue>> timeStampValueMap = new HashMap<>();
            long timestamp = 0;
            for (Map.Entry<String, List<QueryResult>> item : map.entrySet()) {
                //范围查询
                if (timeLowerBound != -1 && timeUpperBound != -1) {
                    for (QueryResult queryResult : item.getValue()) {
                        if (queryResult.getTimestamp() >= timeLowerBound && queryResult.getTimestamp() < timeUpperBound) {
                            Map<String, ColumnValue> columnValueMap = timeStampValueMap.get(queryResult.getTimestamp());
                            if (columnValueMap == null) {
                                columnValueMap = new HashMap<>();
                                columnValueMap.put(item.getKey(), queryResult.getColumnValue());
                                timeStampValueMap.put(queryResult.getTimestamp(), columnValueMap);
                            } else {
                                columnValueMap.put(item.getKey(), queryResult.getColumnValue());
                            }
                        }
                    }
                } else {
                    //最新值查询
                    Optional<QueryResult> first = item.getValue().stream().max(Comparator.comparingLong(QueryResult::getTimestamp));
                    if (first.isPresent()) {
                        QueryResult queryResult = first.get();
                        latestQueryColumns.put(item.getKey(), queryResult.getColumnValue());
                        if (queryResult.getTimestamp() > timestamp) {
                            timestamp = queryResult.getTimestamp();
                        }
                    }
                }
            }
            if (latestQueryColumns.size() > 0) {
                Row row = new Row(entity.getKey(), timestamp, latestQueryColumns);
                result.add(row);
            }
            if (timeStampValueMap.size() > 0) {
                for (Map.Entry<Long, Map<String, ColumnValue>> e : timeStampValueMap.entrySet()) {
                    Row row = new Row(entity.getKey(), e.getKey(), e.getValue());
                    result.add(row);
                }
            }
        }
        return result;
    }
}
