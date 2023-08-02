package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.impl.bpluse.BTree;
import com.alibaba.lindorm.contest.structs.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DataQueryHandler {
    private final FileManager fileManager;

    public DataQueryHandler(FileManager fileManager) {
        this.fileManager = fileManager;
    }

    public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        ConcurrentHashMap<String, BTree<Long>> indexBlockMap = IndexLoader.getIndexBlockMap(pReadReq.getTableName());
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
        ConcurrentHashMap<String, BTree<Long>> indexBlockMap = IndexLoader.getIndexBlockMap(trReadReq.getTableName());
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
        ConcurrentHashMap<String, BTree<Long>> indexBlockMap = IndexLoader.getIndexBlockMap(tableName);

        //先过滤数据
        List<Index> queryLatestList = new ArrayList<>();
        Map<String, Index> queryRangeMap = new HashMap<>();
        for (Vin vin : vinList) {
            String rowKey = new String(vin.getVin());
            BTree<Long> tree = indexBlockMap.get(rowKey);
            if (tree != null) {
                //范围查询
                if (timeLowerBound != -1 && timeUpperBound != -1) {
                    List<Object> list = tree.searchRange(timeLowerBound, timeUpperBound);
                    for (Object o : list) {
                        queryLatestList.add((Index) o);
                    }
                } else {
                    queryRangeMap.put(rowKey, (Index) tree.searchMax(Long.MAX_VALUE));
                }
            }
        }
        List<Index> r;
        if (queryRangeMap.size() > 0) {
            r = new ArrayList<>(queryRangeMap.values());
        } else {
            r = queryLatestList;
        }

        Map<Integer, FileChannel> fileChannelMap = fileManager.getReadFileMap().get(tableName);
        SchemaMeta schemaMeta = fileManager.getSchemaMeta(tableName);
        Map<String, Vin> vinNameMap = vinList.stream().collect(Collectors.toMap(i -> new String(i.getVin()), i -> i));
        ArrayList<Row> rowList = new ArrayList<>();
        for (Index index : r) {
            String rowKeyName = new String(index.getRowKey());
            if (vinNameMap.containsKey(rowKeyName)) {
                Vin vin = vinNameMap.get(rowKeyName);
                int folderIndex = vin.hashCode() % CommonSetting.NUM_FOLDERS;
                FileChannel fileChannel = fileChannelMap.get(folderIndex);
                if (fileChannel.size() == 0) {
                    continue;
                }
                MappedByteBuffer sizeByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
                while (sizeByteBuffer.hasRemaining()) {
                    long t = sizeByteBuffer.getLong();
                    Map<String, ColumnValue> columns = new HashMap<>();

                    for (int cI = 0; cI < schemaMeta.getColumnsNum(); ++cI) {
                        String cName = schemaMeta.getColumnsName().get(cI);
                        ColumnValue.ColumnType cType = schemaMeta.getColumnsType().get(cI);
                        ColumnValue cVal;
                        switch (cType) {
                            case COLUMN_TYPE_INTEGER:
                                int intVal = sizeByteBuffer.getInt();
                                cVal = new ColumnValue.IntegerColumn(intVal);
                                break;
                            case COLUMN_TYPE_DOUBLE_FLOAT:
                                double doubleVal = sizeByteBuffer.getDouble();
                                cVal = new ColumnValue.DoubleFloatColumn(doubleVal);
                                break;
                            case COLUMN_TYPE_STRING:
                                int length = sizeByteBuffer.getInt();
                                byte[] bytes = new byte[length];
                                for (int i = 0; i < length; i++) {
                                    bytes[i] = sizeByteBuffer.get();
                                }
                                cVal = new ColumnValue.StringColumn(ByteBuffer.wrap(bytes));
                                break;
                            default:
                                throw new IllegalStateException("Undefined column type, this is not expected");
                        }
                        columns.put(cName, cVal);
                    }
                    //构建Row
                    Row row = new Row(vin, t, columns);
                    rowList.add(row);
                }
            }
        }
        return rowList;
    }
}
