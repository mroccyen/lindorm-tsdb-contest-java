package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.impl.bpluse.BTree;
import com.alibaba.lindorm.contest.structs.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.READ;

public class QueryHandler {
    private final Map<Integer, File> dpFileMap;

    public QueryHandler(Map<Integer, File> dpFileMap) {
        this.dpFileMap = dpFileMap;
    }

    public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        ConcurrentHashMap<String, BTree<Long>> indexBlockMap = IndexBufferHandler.getIndexBlockMap(pReadReq.getTableName());
        int size = indexBlockMap.size();
        //System.out.println(">>> executeLatestQuery " + pReadReq.getTableName() + " exist index data size: " + size);
        long start = System.currentTimeMillis();
        String tableName = pReadReq.getTableName();
        Collection<Vin> vinList = pReadReq.getVins();
        Set<String> requestedColumns = pReadReq.getRequestedColumns();
        ArrayList<Row> result;
        try {
            result = query(tableName, vinList, requestedColumns, -1, -1);
        } catch (Exception ex) {
            System.out.println(">>> executeLatestQuery happen exception: " + ex.getMessage());
            for (StackTraceElement stackTraceElement : ex.getStackTrace()) {
                System.out.println(">>> executeLatestQuery happen exception: " + stackTraceElement.toString());
            }
            throw new IOException(ex);
        }
        long end = System.currentTimeMillis();
        //System.out.println(">>> executeLatestQuery time: " + (end - start));
        return result;
    }

    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        ConcurrentHashMap<String, BTree<Long>> indexBlockMap = IndexBufferHandler.getIndexBlockMap(trReadReq.getTableName());
        int size = indexBlockMap.size();
        //System.out.println(">>> executeTimeRangeQuery " + trReadReq.getTableName() + " exist index data size: " + size);
        long start = System.currentTimeMillis();
        String tableName = trReadReq.getTableName();
        Vin vin = trReadReq.getVin();
        Set<String> requestedColumns = trReadReq.getRequestedFields();
        ArrayList<Row> result;
        try {
            result = query(tableName, Collections.singletonList(vin), requestedColumns, trReadReq.getTimeLowerBound(), trReadReq.getTimeUpperBound());
        } catch (Exception ex) {
            System.out.println(">>> executeTimeRangeQuery happen exception: " + ex.getMessage());
            for (StackTraceElement stackTraceElement : ex.getStackTrace()) {
                System.out.println(">>> executeTimeRangeQuery happen exception: " + stackTraceElement.toString());
            }
            throw new IOException(ex);
        }
        long end = System.currentTimeMillis();
        //System.out.println(">>> executeTimeRangeQuery time: " + (end - start));
        return result;
    }

    private ArrayList<Row> query(String tableName, Collection<Vin> vinList, Set<String> requestedColumns, long timeLowerBound, long timeUpperBound) throws IOException {
        //获取当前表所有的索引信息
        ConcurrentHashMap<String, BTree<Long>> indexBlockMap = IndexBufferHandler.getIndexBlockMap(tableName);

        //先过滤数据
        List<IndexBlock> queryLatestList = new ArrayList<>();
        Map<String, IndexBlock> queryRangeMap = new HashMap<>();
        for (Vin vin : vinList) {
            String rowKey = new String(vin.getVin());
            BTree<Long> tree = indexBlockMap.get(rowKey);
            if (tree != null) {
                //范围查询
                if (timeLowerBound != -1 && timeUpperBound != -1) {
                    List<Object> list = tree.searchRange(timeLowerBound, timeUpperBound);
                    for (Object o : list) {
                        queryLatestList.add((IndexBlock) o);
                    }
                } else {
                    queryRangeMap.put(rowKey, (IndexBlock) tree.searchMax(Long.MAX_VALUE));
                }
            }
        }
        List<IndexBlock> r;
        if (queryRangeMap.size() > 0) {
            r = new ArrayList<>(queryRangeMap.values());
        } else {
            r = queryLatestList;
        }

        Map<Integer, FileChannel> fileChannelMap = new TreeMap<>();
        for (Map.Entry<Integer, File> fileEntry : dpFileMap.entrySet()) {
            FileChannel fileChannel = FileChannel.open(fileEntry.getValue().toPath(), READ);
            fileChannelMap.put(fileEntry.getKey(), fileChannel);
        }
        Map<String, Vin> vinNameMap = vinList.stream().collect(Collectors.toMap(i -> new String(i.getVin()), i -> i));
        ArrayList<Row> rowList = new ArrayList<>();
        for (IndexBlock indexBlock : r) {
            String rowKeyName = new String(indexBlock.getRowKey());
            Map<String, ColumnValue> columns = new HashMap<>();
            if (vinNameMap.containsKey(rowKeyName)) {
                Vin vin = vinNameMap.get(rowKeyName);
                long t = indexBlock.getTimestamp();

                ByteBuffer sizeByteBuffer = ByteBuffer.allocateDirect(indexBlock.getDataSize());
                FileChannel fileChannel = fileChannelMap.get((int) indexBlock.getIndex());
                if (fileChannel.size() == 0) {
                    continue;
                }
                fileChannel.read(sizeByteBuffer, indexBlock.getOffset());
                sizeByteBuffer.flip();
                int bufferPosition = sizeByteBuffer.position();
                int bufferLimit = sizeByteBuffer.limit();
                while (bufferPosition < bufferLimit) {
                    int rowKeyLength = Vin.VIN_LENGTH;
                    byte[] rowKey = new byte[rowKeyLength];
                    for (int i = 0; i < rowKeyLength; i++) {
                        rowKey[i] = sizeByteBuffer.get();
                    }
                    String existRowKey = new String(rowKey);
                    int columnNameLength = sizeByteBuffer.get();
                    byte[] columnName = new byte[columnNameLength];
                    for (int i = 0; i < columnNameLength; i++) {
                        columnName[i] = sizeByteBuffer.get();
                    }
                    String existColumnName = new String(columnName);
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
                //构建Row
                Row row = new Row(vin, t, columns);
                rowList.add(row);
            }
        }
        for (Map.Entry<Integer, FileChannel> fileChannelEntry : fileChannelMap.entrySet()) {
            fileChannelEntry.getValue().close();
        }

        return rowList;
    }
}
