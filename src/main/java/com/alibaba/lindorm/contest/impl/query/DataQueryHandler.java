package com.alibaba.lindorm.contest.impl.query;

import com.alibaba.lindorm.contest.impl.common.CommonSetting;
import com.alibaba.lindorm.contest.impl.file.FileManager;
import com.alibaba.lindorm.contest.impl.index.Index;
import com.alibaba.lindorm.contest.impl.index.IndexLoader;
import com.alibaba.lindorm.contest.impl.schema.SchemaMeta;
import com.alibaba.lindorm.contest.impl.store.ByteBuffersDataInput;
import com.alibaba.lindorm.contest.structs.Aggregator;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.LatestQueryRequest;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.TimeRangeAggregationRequest;
import com.alibaba.lindorm.contest.structs.TimeRangeDownsampleRequest;
import com.alibaba.lindorm.contest.structs.TimeRangeQueryRequest;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DataQueryHandler {
    private final FileManager fileManager;

    public DataQueryHandler(FileManager fileManager) {
        this.fileManager = fileManager;
    }

    public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        Collection<Vin> vinList = pReadReq.getVins();
        if (vinList == null || vinList.size() == 0) {
            return new ArrayList<>();
        }
        ArrayList<Row> result;
        try {
            result = doExecuteLatestQuery(pReadReq);
        } catch (Exception ex) {
            System.out.println(">>> executeLatestQuery happen exception: " + ex.getClass().getName());
            for (StackTraceElement stackTraceElement : ex.getStackTrace()) {
                System.out.println(">>> executeLatestQuery happen exception: " + stackTraceElement.toString());
            }
            throw new IOException(ex);
        }
        return result;
    }

    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        Vin vin = trReadReq.getVin();
        if (vin == null) {
            return new ArrayList<>();
        }
        ArrayList<Row> result;
        try {
            result = doExecuteTimeRangeQuery(trReadReq);
        } catch (Exception ex) {
            System.out.println(">>> executeTimeRangeQuery happen exception: " + ex.getClass().getName());
            for (StackTraceElement stackTraceElement : ex.getStackTrace()) {
                System.out.println(">>> executeTimeRangeQuery happen exception: " + stackTraceElement.toString());
            }
            throw new IOException(ex);
        }
        return result;
    }

    public ArrayList<Row> executeAggregateQuery(TimeRangeAggregationRequest aggregationReq) throws IOException {
        Vin vin = aggregationReq.getVin();
        if (vin == null) {
            return new ArrayList<>();
        }
        ArrayList<Row> result;
        try {
            result = doExecuteAggregateQuery(aggregationReq);
        } catch (Exception ex) {
            System.out.println(">>> executeAggregateQuery happen exception: " + ex.getClass().getName());
            for (StackTraceElement stackTraceElement : ex.getStackTrace()) {
                System.out.println(">>> executeAggregateQuery happen exception: " + stackTraceElement.toString());
            }
            throw new IOException(ex);
        }
        return result;
    }

    public ArrayList<Row> executeDownsampleQuery(TimeRangeDownsampleRequest downsampleReq) throws IOException {
        return new ArrayList<>();
    }

    private ArrayList<Row> doExecuteLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        String tableName = pReadReq.getTableName();
        Collection<Vin> vinList = pReadReq.getVins();
        Set<String> requestedColumns = pReadReq.getRequestedColumns();
        SchemaMeta schemaMeta = fileManager.getSchemaMeta(tableName);

        Map<Vin, Row> latestRowMap = new HashMap<>();
        for (Vin vin : vinList) {
            Index latestIndex = IndexLoader.getLatestIndex(tableName, vin);
            if (latestIndex == null) {
                continue;
            }
            byte[] bytes = latestIndex.getBuffer().array();
            ByteBuffersDataInput tempDataInput = new ByteBuffersDataInput(Collections.singletonList(ByteBuffer.wrap(bytes)));

            Map<String, ColumnValue> columns = getColumns(schemaMeta, tempDataInput, requestedColumns);
            //构建Row
            latestRowMap.put(vin, new Row(vin, CommonSetting.DEFAULT_TIMESTAMP + latestIndex.getDelta(), columns));
        }
        return new ArrayList<>(latestRowMap.values());
    }

    private ArrayList<Row> doExecuteTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        String tableName = trReadReq.getTableName();
        Vin vin = trReadReq.getVin();
        Set<String> requestedColumns = trReadReq.getRequestedColumns();
        long timeLowerBound = trReadReq.getTimeLowerBound();
        long timeUpperBound = trReadReq.getTimeUpperBound();

        SchemaMeta schemaMeta = fileManager.getSchemaMeta(tableName);
        ArrayList<Row> rowList = new ArrayList<>();

        FileChannel fileChannel = fileManager.getReadFileChannel(tableName, vin);
        if (fileChannel == null || fileChannel.size() == 0) {
            return new ArrayList<>();
        }
        MappedByteBuffer sizeByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        ByteBuffersDataInput dataInput = new ByteBuffersDataInput(Collections.singletonList(sizeByteBuffer));
        while (dataInput.position() < dataInput.size()) {
            long delta = dataInput.readVLong();
            long t = CommonSetting.DEFAULT_TIMESTAMP + delta;
            long size = dataInput.readVLong();
            long position = dataInput.position() + size;
            if (t >= timeLowerBound && t < timeUpperBound) {
                ByteBuffer tempBuffer = ByteBuffer.allocate((int) size);
                dataInput.readBytes(tempBuffer, (int) size);
                tempBuffer.flip();
                ByteBuffersDataInput tempDataInput = new ByteBuffersDataInput(Collections.singletonList(ByteBuffer.wrap(tempBuffer.array())));

                Map<String, ColumnValue> columns = getColumns(schemaMeta, tempDataInput, requestedColumns);
                Row row = new Row(vin, t, columns);
                rowList.add(row);
            } else {
                dataInput.seek(position);
            }
        }
        return rowList;
    }

    private ArrayList<Row> doExecuteAggregateQuery(TimeRangeAggregationRequest trReadReq) throws IOException {
        String tableName = trReadReq.getTableName();
        Vin vin = trReadReq.getVin();
        String columnName = trReadReq.getColumnName();
        long timeLowerBound = trReadReq.getTimeLowerBound();
        long timeUpperBound = trReadReq.getTimeUpperBound();

        FileChannel fileChannel = fileManager.getReadFileChannel(tableName, vin);
        if (fileChannel == null || fileChannel.size() == 0) {
            return new ArrayList<>();
        }
        SchemaMeta schemaMeta = fileManager.getSchemaMeta(tableName);
        Aggregator aggregator = trReadReq.getAggregator();
        ColumnValue.ColumnType columnType = getColumnType(schemaMeta, columnName);
        int maxInt = 0;
        int totalInt = 0;
        int totalCountInt = 0;
        double maxDouble = 0;
        double totalDouble = 0;
        int totalCountDouble = 0;
        MappedByteBuffer sizeByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        ByteBuffersDataInput dataInput = new ByteBuffersDataInput(Collections.singletonList(sizeByteBuffer));
        while (dataInput.position() < dataInput.size()) {
            long delta = dataInput.readVLong();
            long t = CommonSetting.DEFAULT_TIMESTAMP + delta;
            long size = dataInput.readVLong();
            long position = dataInput.position() + size;
            if (t >= timeLowerBound && t < timeUpperBound) {
                ByteBuffer tempBuffer = ByteBuffer.allocate((int) size);
                dataInput.readBytes(tempBuffer, (int) size);
                tempBuffer.flip();
                ByteBuffersDataInput tempDataInput = new ByteBuffersDataInput(Collections.singletonList(ByteBuffer.wrap(tempBuffer.array())));
                Map<String, ColumnValue> columns = getColumns(schemaMeta, tempDataInput, Collections.singleton(columnName));
                ColumnValue columnValue = columns.get(columnName);
                if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)) {
                    int integerValue = columnValue.getIntegerValue();
                    totalInt += integerValue;
                    totalCountInt++;
                    if (maxInt > integerValue) {
                        maxInt = integerValue;
                    }
                }
                if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
                    double doubleFloatValue = columnValue.getDoubleFloatValue();
                    totalDouble += doubleFloatValue;
                    totalCountDouble++;
                    if (maxDouble > doubleFloatValue) {
                        maxDouble = doubleFloatValue;
                    }
                }
            } else {
                dataInput.seek(position);
            }
        }
        ArrayList<Row> rowList = new ArrayList<>();
        if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)) {
            if (aggregator.equals(Aggregator.MAX)) {
                Map<String, ColumnValue> columns = new HashMap<>();
                columns.put(columnName, new ColumnValue.IntegerColumn(maxInt));
                Row row = new Row(vin, timeLowerBound, columns);
                rowList.add(row);
            }
            if (aggregator.equals(Aggregator.AVG)) {
                int avg = 0;
                if (totalCountInt != 0) {
                    avg = totalInt / totalCountInt;
                }
                Map<String, ColumnValue> columns = new HashMap<>();
                columns.put(columnName, new ColumnValue.IntegerColumn(avg));
                Row row = new Row(vin, timeLowerBound, columns);
                rowList.add(row);
            }
        }
        if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
            if (aggregator.equals(Aggregator.MAX)) {
                Map<String, ColumnValue> columns = new HashMap<>();
                columns.put(columnName, new ColumnValue.DoubleFloatColumn(maxDouble));
                Row row = new Row(vin, timeLowerBound, columns);
                rowList.add(row);
            }
            if (aggregator.equals(Aggregator.AVG)) {
                double avg = 0;
                if (totalCountDouble != 0) {
                    avg = totalDouble / totalCountDouble;
                }
                Map<String, ColumnValue> columns = new HashMap<>();
                columns.put(columnName, new ColumnValue.DoubleFloatColumn(avg));
                Row row = new Row(vin, timeLowerBound, columns);
                rowList.add(row);
            }
        }
        return rowList;
    }

    private Map<String, ColumnValue> getColumns(SchemaMeta schemaMeta, ByteBuffersDataInput tempDataInput, Set<String> requestedColumns) throws IOException {
        Map<String, ColumnValue> columns = new HashMap<>();
        ArrayList<String> integerColumnsNameList = schemaMeta.getIntegerColumnsName();
        for (String cName : integerColumnsNameList) {
            int intVal = tempDataInput.readVInt();
            ColumnValue cVal = new ColumnValue.IntegerColumn(intVal);
            if (requestedColumns.contains(cName)) {
                columns.put(cName, cVal);
            }
        }
        ArrayList<String> doubleColumnsNameList = schemaMeta.getDoubleColumnsName();
        for (String cName : doubleColumnsNameList) {
            double doubleVal = tempDataInput.readZDouble();
            ColumnValue cVal = new ColumnValue.DoubleFloatColumn(doubleVal);
            if (requestedColumns.contains(cName)) {
                columns.put(cName, cVal);
            }
        }
        ArrayList<String> stringColumnsNameList = schemaMeta.getStringColumnsName();
        for (String cName : stringColumnsNameList) {
            String s = tempDataInput.readString();
            ColumnValue cVal = new ColumnValue.StringColumn(ByteBuffer.wrap(s.getBytes()));
            if (requestedColumns.contains(cName)) {
                columns.put(cName, cVal);
            }
        }
        return columns;
    }

    private ColumnValue.ColumnType getColumnType(SchemaMeta schemaMeta, String columnName) {
        ArrayList<String> integerColumnsNameList = schemaMeta.getIntegerColumnsName();
        if (integerColumnsNameList.contains(columnName)) {
            return ColumnValue.ColumnType.COLUMN_TYPE_INTEGER;
        }
        ArrayList<String> doubleColumnsNameList = schemaMeta.getDoubleColumnsName();
        if (doubleColumnsNameList.contains(columnName)) {
            return ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT;
        }
        ArrayList<String> stringColumnsNameList = schemaMeta.getStringColumnsName();
        if (stringColumnsNameList.contains(columnName)) {
            return ColumnValue.ColumnType.COLUMN_TYPE_STRING;
        }
        throw new RuntimeException("column type error");
    }
}
