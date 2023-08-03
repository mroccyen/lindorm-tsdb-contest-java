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
        return result;
    }

    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
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
        return result;
    }

    private ArrayList<Row> query(String tableName, Collection<Vin> vinList, Set<String> requestedColumns, long timeLowerBound, long timeUpperBound) throws IOException {
        if (vinList == null || vinList.size() == 0) {
            return new ArrayList<>();
        }
        Map<Integer, FileChannel> fileChannelMap = fileManager.getReadFileMap().get(tableName);
        SchemaMeta schemaMeta = fileManager.getSchemaMeta(tableName);
        ArrayList<Row> rowList = new ArrayList<>();

        Map<Vin, Long> latestTimestamp = new HashMap<>();
        Map<Vin, Row> latestRowMap = new HashMap<>();
        for (Vin vin : vinList) {
            latestTimestamp.put(vin, 0L);
        }
        for (Vin vin : vinList) {
            int folderIndex = vin.hashCode() % CommonSetting.NUM_FOLDERS;
            String vinReqStr = new String(vin.getVin());
            FileChannel fileChannel = fileChannelMap.get(folderIndex);
            if (fileChannel == null || fileChannel.size() == 0) {
                continue;
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
                if (!vinReqStr.equals(vinStr)) {
                    continue;
                }
                //范围查询
                if (timeLowerBound != -1 && timeUpperBound != -1) {
                    if (t >= timeLowerBound && t < timeUpperBound) {
                        //构建Row
                        Row row = new Row(vin, t, columns);
                        rowList.add(row);
                    }
                } else {
                    if (latestTimestamp.get(vin) < t) {
                        //构建Row
                        latestRowMap.put(vin, new Row(vin, t, columns));
                        latestTimestamp.put(vin, t);
                    }
                }
            }
        }
        if (timeLowerBound != -1 && timeUpperBound != -1) {
            return rowList;
        } else {
            return new ArrayList<>(latestRowMap.values());
        }
    }
}
