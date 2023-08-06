package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class DataQueryHandler {
    private final FileManager fileManager;

    public DataQueryHandler(FileManager fileManager) {
        this.fileManager = fileManager;
    }

    public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        String tableName = pReadReq.getTableName();
        Collection<Vin> vinList = pReadReq.getVins();
        if (vinList == null || vinList.size() == 0) {
            return new ArrayList<>();
        }
        Set<String> requestedColumns = pReadReq.getRequestedColumns();
        ArrayList<Row> result;
        try {
            result = executeLatestQuery(tableName, vinList, requestedColumns);
        } catch (Exception ex) {
            System.out.println(">>> executeLatestQuery happen exception: " + ex.getMessage());
            for (StackTraceElement stackTraceElement : ex.getStackTrace()) {
                System.out.println(">>> executeLatestQuery happen exception: " + stackTraceElement.toString());
            }
            throw new IOException(ex);
        }
        return result;
    }

    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        String tableName = trReadReq.getTableName();
        Vin vin = trReadReq.getVin();
        if (vin == null) {
            return new ArrayList<>();
        }
        Set<String> requestedColumns = trReadReq.getRequestedFields();
        ArrayList<Row> result;
        try {
            result = executeTimeRangeQuery(tableName, vin, requestedColumns, trReadReq.getTimeLowerBound(), trReadReq.getTimeUpperBound());
        } catch (Exception ex) {
            System.out.println(">>> executeTimeRangeQuery happen exception: " + ex.getMessage());
            for (StackTraceElement stackTraceElement : ex.getStackTrace()) {
                System.out.println(">>> executeTimeRangeQuery happen exception: " + stackTraceElement.toString());
            }
            throw new IOException(ex);
        }
        return result;
    }

    private ArrayList<Row> executeLatestQuery(String tableName, Collection<Vin> vinList, Set<String> requestedColumns) throws IOException {
        SchemaMeta schemaMeta = fileManager.getSchemaMeta(tableName);

        Map<Vin, Long> latestTimestamp = new HashMap<>();
        Map<Vin, Row> latestRowMap = new HashMap<>();
        Set<String> vinNameSet = new HashSet<>();
        for (Vin vin : vinList) {
            latestTimestamp.put(vin, 0L);
            String vinReqStr = new String(vin.getVin());
            vinNameSet.add(vinReqStr);
        }
        ByteBuffer sizeByteBuffer = ByteBuffer.allocate(1024 * 10);
        for (Vin vin : vinList) {
            FileChannel fileChannel = fileManager.getReadFileChannel(tableName, vin);
            if (fileChannel == null || fileChannel.size() == 0) {
                continue;
            }
            if (fileChannel.size() == 0) {
                fileChannel.close();
                continue;
            }
            Index latestIndex = IndexLoader.getLatestIndex(tableName, vin);
            if (latestIndex == null) {
                return null;
            }
            fileChannel.read(sizeByteBuffer, latestIndex.getOffset());
            sizeByteBuffer.flip();

            byte[] vinBytes = new byte[Vin.VIN_LENGTH];
            for (int i = 0; i < Vin.VIN_LENGTH; i++) {
                vinBytes[i] = sizeByteBuffer.get();
            }
            String vinStr = new String(vinBytes);
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
                if (requestedColumns.contains(cName)) {
                    columns.put(cName, cVal);
                }
            }
            if (vinNameSet.contains(vinStr)) {
                Vin v = new Vin(vinBytes);
                if (latestTimestamp.get(v) < t) {
                    //构建Row
                    latestRowMap.put(v, new Row(v, t, columns));
                    latestTimestamp.put(v, t);
                }
            }
            sizeByteBuffer.clear();
            fileChannel.close();
        }
        return new ArrayList<>(latestRowMap.values());
    }

    private ArrayList<Row> executeTimeRangeQuery(String tableName, Vin vin, Set<String> requestedColumns, long timeLowerBound, long timeUpperBound) throws IOException {
        SchemaMeta schemaMeta = fileManager.getSchemaMeta(tableName);
        ArrayList<Row> rowList = new ArrayList<>();

        Map<Vin, ArrayList<Row>> timeRangeRowMap = new HashMap<>();
        Set<String> vinNameSet = new HashSet<>();
        String vinReqStr = new String(vin.getVin());
        vinNameSet.add(vinReqStr);

        FileChannel fileChannel = fileManager.getReadFileChannel(tableName, vin);
        if (fileChannel == null) {
            return new ArrayList<>();
        }
        if (fileChannel.size() == 0) {
            fileChannel.close();
            return new ArrayList<>();
        }
        MappedByteBuffer sizeByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        while (sizeByteBuffer.hasRemaining()) {
            byte[] vinBytes = new byte[Vin.VIN_LENGTH];
            for (int i = 0; i < Vin.VIN_LENGTH; i++) {
                vinBytes[i] = sizeByteBuffer.get();
            }
            String vinStr = new String(vinBytes);
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
                if (requestedColumns.contains(cName)) {
                    columns.put(cName, cVal);
                }
            }
            if (vinNameSet.contains(vinStr)) {
                //范围查询
                if (t >= timeLowerBound && t < timeUpperBound) {
                    //构建Row
                    Vin v = new Vin(vinBytes);
                    ArrayList<Row> rows = timeRangeRowMap.get(v);
                    if (rows == null) {
                        rows = new ArrayList<>();
                    }
                    Row row = new Row(v, t, columns);
                    rows.add(row);
                    timeRangeRowMap.put(v, rows);
                }
            }
        }
        if (timeLowerBound != -1 && timeUpperBound != -1) {
            for (Map.Entry<Vin, ArrayList<Row>> e : timeRangeRowMap.entrySet()) {
                rowList.addAll(e.getValue());
            }
        }

        fileChannel.close();
        return rowList;
    }
}
