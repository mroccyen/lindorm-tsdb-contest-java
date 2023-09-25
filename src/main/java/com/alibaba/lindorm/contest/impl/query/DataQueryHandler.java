package com.alibaba.lindorm.contest.impl.query;

import com.alibaba.lindorm.contest.impl.common.CommonSetting;
import com.alibaba.lindorm.contest.impl.file.FileManager;
import com.alibaba.lindorm.contest.impl.index.Index;
import com.alibaba.lindorm.contest.impl.index.IndexLoader;
import com.alibaba.lindorm.contest.impl.schema.SchemaMeta;
import com.alibaba.lindorm.contest.impl.store.ByteBuffersDataInput;
import com.alibaba.lindorm.contest.structs.Aggregator;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;
import com.alibaba.lindorm.contest.structs.LatestQueryRequest;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.TimeRangeAggregationRequest;
import com.alibaba.lindorm.contest.structs.TimeRangeDownsampleRequest;
import com.alibaba.lindorm.contest.structs.TimeRangeQueryRequest;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.Buffer;
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
            System.out.println(">>> executeLatestQuery happen exception: " + ex.getMessage());
            System.out.println(">>> executeLatestQuery happen exception: " + ex.getClass().getName());
            for (StackTraceElement stackTraceElement : ex.getStackTrace()) {
                System.out.println(">>> executeLatestQuery happen exception: " + stackTraceElement.toString());
            }
            throw new IOException(ex);
        }
        return result;
    }

    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        handleFlush();
        Vin vin = trReadReq.getVin();
        if (vin == null) {
            return new ArrayList<>();
        }
        ArrayList<Row> result;
        try {
            result = doExecuteTimeRangeQuery(trReadReq);
        } catch (Exception ex) {
            System.out.println(">>> executeTimeRangeQuery happen exception: " + ex.getMessage());
            System.out.println(">>> executeTimeRangeQuery happen exception: " + ex.getClass().getName());
            for (StackTraceElement stackTraceElement : ex.getStackTrace()) {
                System.out.println(">>> executeTimeRangeQuery happen exception: " + stackTraceElement.toString());
            }
            throw new IOException(ex);
        }
        return result;
    }

    public ArrayList<Row> executeAggregateQuery(TimeRangeAggregationRequest aggregationReq) throws IOException {
        handleFlush();
        Vin vin = aggregationReq.getVin();
        if (vin == null) {
            return new ArrayList<>();
        }
        ArrayList<Row> result;
        try {
            result = doExecuteAggregateQuery(aggregationReq);
        } catch (Exception ex) {
            System.out.println(">>> executeAggregateQuery happen exception: " + ex.getMessage());
            System.out.println(">>> executeAggregateQuery happen exception: " + ex.getClass().getName());
            for (StackTraceElement stackTraceElement : ex.getStackTrace()) {
                System.out.println(">>> executeAggregateQuery happen exception: " + stackTraceElement.toString());
            }
            throw new IOException(ex);
        }
        return result;
    }

    public ArrayList<Row> executeDownsampleQuery(TimeRangeDownsampleRequest downsampleReq) throws IOException {
        handleFlush();
        Vin vin = downsampleReq.getVin();
        if (vin == null) {
            return new ArrayList<>();
        }
        ArrayList<Row> result;
        try {
            result = doExecuteDownsampleQuery(downsampleReq);
        } catch (Exception ex) {
            System.out.println(">>> executeDownsampleQuery happen exception: " + ex.getMessage());
            System.out.println(">>> executeDownsampleQuery happen exception: " + ex.getClass().getName());
            for (StackTraceElement stackTraceElement : ex.getStackTrace()) {
                System.out.println(">>> executeDownsampleQuery happen exception: " + stackTraceElement.toString());
            }
            throw new IOException(ex);
        }
        return result;
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
            latestRowMap.put(vin, new Row(vin, latestIndex.getTimestamp(), columns));
        }
        for (Map.Entry<Vin, Row> entry : latestRowMap.entrySet()) {
            System.out.println(">>> doExecuteLatestQuery key：" + entry.getValue().toString() + "，result：" + entry.getValue().toString());
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
        Map<Long, Map<String, ColumnValue>> columnMap = new HashMap<>();

        ByteBuffer vinBuffer = ByteBuffer.allocate(Vin.VIN_LENGTH);
        for (String requestedColumn : requestedColumns) {
            FileChannel fileChannel = fileManager.getReadFileChannel(tableName, requestedColumn);
            if (fileChannel == null || fileChannel.size() == 0) {
                continue;
            }
            MappedByteBuffer sizeByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            ByteBuffersDataInput dataInput = new ByteBuffersDataInput(Collections.singletonList(sizeByteBuffer));
            while (dataInput.position() < dataInput.size()) {
                dataInput.readBytes(vinBuffer, Vin.VIN_LENGTH);
                long t = dataInput.readVLong();
                Map<String, ColumnValue> columns = getColumn(schemaMeta, dataInput, requestedColumn);
                vinBuffer.flip();
                if (vin.equals(new Vin(vinBuffer.array()))) {
                    if (t >= timeLowerBound && t < timeUpperBound) {
                        if (columns.size() > 0) {
                            Map<String, ColumnValue> map = columnMap.get(t);
                            if (map == null) {
                                columnMap.put(t, columns);
                            } else {
                                map.putAll(columns);
                            }
                        }
                    }
                }
                vinBuffer.clear();
            }
        }
        for (Map.Entry<Long, Map<String, ColumnValue>> entry : columnMap.entrySet()) {
            Row row = new Row(vin, entry.getKey(), entry.getValue());
            rowList.add(row);
        }
        for (Row row : rowList) {
            System.out.println(">>> doExecuteTimeRangeQuery result：" + row.toString());
        }
        return rowList;
    }

    private ArrayList<Row> doExecuteAggregateQuery(TimeRangeAggregationRequest trReadReq) throws IOException {
        String tableName = trReadReq.getTableName();
        Vin vin = trReadReq.getVin();
        String columnName = trReadReq.getColumnName();
        long timeLowerBound = trReadReq.getTimeLowerBound();
        long timeUpperBound = trReadReq.getTimeUpperBound();

        FileChannel fileChannel = fileManager.getReadFileChannel(tableName, columnName);
        if (fileChannel == null || fileChannel.size() == 0) {
            System.out.println(">>> doExecuteAggregateQuery fileChannel have not data");
            return new ArrayList<>();
        }
        SchemaMeta schemaMeta = fileManager.getSchemaMeta(tableName);
        Aggregator aggregator = trReadReq.getAggregator();
        ColumnValue.ColumnType columnType = getColumnType(schemaMeta, columnName);
        int maxInt = CommonSetting.INT_MIN;
        boolean hasMaxInt = false;
        BigDecimal totalInt = BigDecimal.ZERO;
        BigDecimal totalCountInt = BigDecimal.ZERO;
        double maxDouble = CommonSetting.DOUBLE_MIN;
        boolean hasMaxDouble = false;
        BigDecimal totalDouble = BigDecimal.ZERO;
        BigDecimal totalCountDouble = BigDecimal.ZERO;
        MappedByteBuffer sizeByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        ByteBuffersDataInput dataInput = new ByteBuffersDataInput(Collections.singletonList(sizeByteBuffer));
        ByteBuffer vinBuffer = ByteBuffer.allocate(Vin.VIN_LENGTH);
        while (dataInput.position() < dataInput.size()) {
            dataInput.readBytes(vinBuffer, Vin.VIN_LENGTH);
            long t = dataInput.readVLong();
            Map<String, ColumnValue> columns = getColumn(schemaMeta, dataInput, columnName);
            vinBuffer.flip();
            if (vin.equals(new Vin(vinBuffer.array()))) {
                if (t >= timeLowerBound && t < timeUpperBound) {
                    ColumnValue columnValue = columns.get(columnName);
                    if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)) {
                        int integerValue = columnValue.getIntegerValue();
                        totalInt = totalInt.add(new BigDecimal(String.valueOf(integerValue)));
                        totalCountInt = totalCountInt.add(BigDecimal.ONE);
                        if (integerValue > maxInt) {
                            maxInt = integerValue;
                        }
                        hasMaxInt = true;
                    }
                    if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
                        double doubleFloatValue = columnValue.getDoubleFloatValue();
                        totalDouble = totalDouble.add(new BigDecimal(doubleFloatValue));
                        totalCountDouble = totalCountDouble.add(BigDecimal.ONE);
                        if (doubleFloatValue > maxDouble) {
                            maxDouble = doubleFloatValue;
                        }
                        hasMaxDouble = true;
                    }
                }
            }
            vinBuffer.clear();
        }
        ArrayList<Row> rowList = new ArrayList<>();
        if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)) {
//            System.out.println(">>> doExecuteAggregateQuery columnName:" + columnName + ", aggregator:" + aggregator.name() + ", result:{" +
//                "hasMaxInt=" + hasMaxInt +
//                ", hasMaxDouble=" + hasMaxDouble +
//                ", timeLowerBound=" + timeLowerBound +
//                ", timeUpperBound=" + timeUpperBound +
//                ", maxInt=" + maxInt +
//                ", totalInt=" + totalInt +
//                ", totalCountInt=" + totalCountInt +
//                ", maxDouble=" + maxDouble +
//                ", totalDouble=" + totalDouble +
//                ", totalCountDouble=" + totalCountDouble +
//                '}');
            if (aggregator.equals(Aggregator.MAX)) {
                if (!hasMaxInt) {
//                    System.out.println(">>> doExecuteAggregateQuery have not max int");
                    return new ArrayList<>();
                }
                Map<String, ColumnValue> columns = new HashMap<>();
                columns.put(columnName, new ColumnValue.IntegerColumn(maxInt));
                Row row = new Row(vin, timeLowerBound, columns);
                rowList.add(row);
            }
            if (aggregator.equals(Aggregator.AVG)) {
                double avg;
                if (totalCountInt.compareTo(BigDecimal.ZERO) != 0) {
                    avg = totalInt.divide(totalCountInt, 12, RoundingMode.HALF_UP).doubleValue();
                } else {
//                    System.out.println(">>> doExecuteAggregateQuery have not total count int");
                    return new ArrayList<>();
                }
                Map<String, ColumnValue> columns = new HashMap<>();
                columns.put(columnName, new ColumnValue.DoubleFloatColumn(avg));
                Row row = new Row(vin, timeLowerBound, columns);
                rowList.add(row);
            }
        }
        if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
            if (aggregator.equals(Aggregator.MAX)) {
                if (!hasMaxDouble) {
//                    System.out.println(">>> doExecuteAggregateQuery have not max double");
                    return new ArrayList<>();
                }
                Map<String, ColumnValue> columns = new HashMap<>();
                columns.put(columnName, new ColumnValue.DoubleFloatColumn(maxDouble));
                Row row = new Row(vin, timeLowerBound, columns);
                rowList.add(row);
            }
            if (aggregator.equals(Aggregator.AVG)) {
                double avg;
                if (totalCountDouble.compareTo(BigDecimal.ZERO) != 0) {
                    avg = totalDouble.divide(totalCountDouble, 12, RoundingMode.HALF_UP).doubleValue();
                } else {
//                    System.out.println(">>> doExecuteAggregateQuery have not total count double");
                    return new ArrayList<>();
                }
                Map<String, ColumnValue> columns = new HashMap<>();
                columns.put(columnName, new ColumnValue.DoubleFloatColumn(avg));
                Row row = new Row(vin, timeLowerBound, columns);
                rowList.add(row);
            }
        }
        return rowList;
    }

    private ArrayList<Row> doExecuteDownsampleQuery(TimeRangeDownsampleRequest trReadReq) throws IOException {
        String tableName = trReadReq.getTableName();
        Vin vin = trReadReq.getVin();
        String columnName = trReadReq.getColumnName();
        FileChannel fileChannel = fileManager.getReadFileChannel(tableName, columnName);
        if (fileChannel == null || fileChannel.size() == 0) {
            System.out.println(">>> doExecuteDownsampleQuery fileChannel have not data");
            return new ArrayList<>();
        }
        long timeLowerBound = trReadReq.getTimeLowerBound();
        long timeUpperBound = trReadReq.getTimeUpperBound();
        long interval = trReadReq.getInterval();
        CompareExpression columnFilter = trReadReq.getColumnFilter();
        //分段
        ArrayList<IntervalInfo> intervalInfoList = new ArrayList<>();
        for (long i = timeLowerBound; i < timeUpperBound; i += interval) {
            IntervalInfo intervalInfo = new IntervalInfo();
            intervalInfo.setTimeLowerBound(i);
            intervalInfo.setTimeUpperBound(i + interval);
            intervalInfoList.add(intervalInfo);
        }

        SchemaMeta schemaMeta = fileManager.getSchemaMeta(tableName);
        Aggregator aggregator = trReadReq.getAggregator();
        ColumnValue.ColumnType columnType = getColumnType(schemaMeta, columnName);
        MappedByteBuffer sizeByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        ByteBuffersDataInput dataInput = new ByteBuffersDataInput(Collections.singletonList(sizeByteBuffer));
        boolean notEmpty = false;
        ByteBuffer vinBuffer = ByteBuffer.allocate(Vin.VIN_LENGTH);
        while (dataInput.position() < dataInput.size()) {
            dataInput.readBytes(vinBuffer, Vin.VIN_LENGTH);
            long t = dataInput.readVLong();
            Map<String, ColumnValue> columns = getColumn(schemaMeta, dataInput, columnName);
            vinBuffer.flip();
            if (vin.equals(new Vin(vinBuffer.array()))) {
                if (t >= timeLowerBound && t < timeUpperBound) {
                    notEmpty = true;
                    IntervalInfo intervalInfo = getIntervalInfo(intervalInfoList, t);
                    if (intervalInfo == null) {
                        continue;
                    }
                    intervalInfo.setHasScanData(true);
                    ColumnValue columnValue = columns.get(columnName);
                    if (columnFilter.doCompare(columnValue)) {
                        if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)) {
                            int integerValue = columnValue.getIntegerValue();
                            BigDecimal totalInt = intervalInfo.getTotalInt();
                            totalInt = totalInt.add(new BigDecimal(integerValue));
                            intervalInfo.setTotalInt(totalInt);
                            BigDecimal totalCountInt = intervalInfo.getTotalCountInt();
                            totalCountInt = totalCountInt.add(BigDecimal.ONE);
                            intervalInfo.setTotalCountInt(totalCountInt);
                            int maxInt = intervalInfo.getMaxInt();
                            if (integerValue > maxInt) {
                                maxInt = integerValue;
                                intervalInfo.setMaxInt(maxInt);
                                intervalInfo.setHasMaxInt(true);
                            }
                        }
                        if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
                            double doubleFloatValue = columnValue.getDoubleFloatValue();
                            BigDecimal totalDouble = intervalInfo.getTotalDouble();
                            totalDouble = totalDouble.add(new BigDecimal(doubleFloatValue));
                            intervalInfo.setTotalDouble(totalDouble);
                            BigDecimal totalCountDouble = intervalInfo.getTotalCountDouble();
                            totalCountDouble = totalCountDouble.add(BigDecimal.ONE);
                            intervalInfo.setTotalCountDouble(totalCountDouble);
                            double maxDouble = intervalInfo.getMaxDouble();
                            if (doubleFloatValue > maxDouble) {
                                maxDouble = doubleFloatValue;
                                intervalInfo.setMaxDouble(maxDouble);
                                intervalInfo.setHasMaxDouble(true);
                            }
                        }
                    }
                }
            }
            vinBuffer.clear();
        }
        ArrayList<Row> rowList = new ArrayList<>();
        if (!notEmpty) {
            return new ArrayList<>();
        }
        for (IntervalInfo intervalInfo : intervalInfoList) {
//            System.out.println(">>> doExecuteDownsampleQuery columnName:" + columnName + ", aggregator:" + aggregator.name() + ", intervalInfo:" + intervalInfo.toString());
            if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)) {
                //没有扫描到任何值
                if (!intervalInfo.hasScanData()) {
                    Map<String, ColumnValue> columns = new HashMap<>();
                    columns.put(columnName, new ColumnValue.IntegerColumn(CommonSetting.INT_NAN));
                    Row row = new Row(vin, intervalInfo.getTimeLowerBound(), columns);
                    rowList.add(row);
                    continue;
                }
                if (aggregator.equals(Aggregator.MAX)) {
                    int maxInt = intervalInfo.getMaxInt();
                    Map<String, ColumnValue> columns = new HashMap<>();
                    columns.put(columnName, new ColumnValue.IntegerColumn(intervalInfo.hasMaxInt() ? maxInt : CommonSetting.INT_NAN));
                    Row row = new Row(vin, intervalInfo.getTimeLowerBound(), columns);
                    rowList.add(row);
                }
                if (aggregator.equals(Aggregator.AVG)) {
                    double avg;
                    if (intervalInfo.getTotalCountInt().compareTo(BigDecimal.ZERO) != 0) {
                        avg = intervalInfo.getTotalInt().divide(intervalInfo.getTotalCountInt(), 12, RoundingMode.HALF_UP).doubleValue();
                    } else {
                        avg = CommonSetting.DOUBLE_NAN;
                    }
                    Map<String, ColumnValue> columns = new HashMap<>();
                    columns.put(columnName, new ColumnValue.DoubleFloatColumn(avg));
                    Row row = new Row(vin, intervalInfo.getTimeLowerBound(), columns);
                    rowList.add(row);
                }
            }
            if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
                //没有扫描到任何值
                if (!intervalInfo.hasScanData()) {
                    Map<String, ColumnValue> columns = new HashMap<>();
                    columns.put(columnName, new ColumnValue.DoubleFloatColumn(CommonSetting.DOUBLE_NAN));
                    Row row = new Row(vin, intervalInfo.getTimeLowerBound(), columns);
                    rowList.add(row);
                    continue;
                }
                if (aggregator.equals(Aggregator.MAX)) {
                    Map<String, ColumnValue> columns = new HashMap<>();
                    double maxDouble = intervalInfo.getMaxDouble();
                    columns.put(columnName, new ColumnValue.DoubleFloatColumn(intervalInfo.hasMaxDouble() ? maxDouble : CommonSetting.DOUBLE_NAN));
                    Row row = new Row(vin, intervalInfo.getTimeLowerBound(), columns);
                    rowList.add(row);
                }
                if (aggregator.equals(Aggregator.AVG)) {
                    double avg;
                    if (intervalInfo.getTotalCountDouble().compareTo(BigDecimal.ZERO) != 0) {
                        avg = intervalInfo.getTotalDouble().divide(intervalInfo.getTotalCountDouble(), 12, RoundingMode.HALF_UP).doubleValue();
                    } else {
                        avg = CommonSetting.DOUBLE_NAN;
                    }
                    Map<String, ColumnValue> columns = new HashMap<>();
                    columns.put(columnName, new ColumnValue.DoubleFloatColumn(avg));
                    Row row = new Row(vin, intervalInfo.getTimeLowerBound(), columns);
                    rowList.add(row);
                }
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

    private Map<String, ColumnValue> getColumn(SchemaMeta schemaMeta, ByteBuffersDataInput tempDataInput, String requestedColumn) throws IOException {
        Map<String, ColumnValue> columns = new HashMap<>();
        ArrayList<String> integerColumnsNameList = schemaMeta.getIntegerColumnsName();
        if (integerColumnsNameList.contains(requestedColumn)) {
            int intVal = tempDataInput.readVInt();
            ColumnValue cVal = new ColumnValue.IntegerColumn(intVal);
            columns.put(requestedColumn, cVal);
        }
        ArrayList<String> doubleColumnsNameList = schemaMeta.getDoubleColumnsName();
        if (doubleColumnsNameList.contains(requestedColumn)) {
            double doubleVal = tempDataInput.readZDouble();
            ColumnValue cVal = new ColumnValue.DoubleFloatColumn(doubleVal);
            columns.put(requestedColumn, cVal);
        }
        ArrayList<String> stringColumnsNameList = schemaMeta.getStringColumnsName();
        if (stringColumnsNameList.contains(requestedColumn)) {
            String s = tempDataInput.readString();
            ColumnValue cVal = new ColumnValue.StringColumn(ByteBuffer.wrap(s.getBytes()));
            columns.put(requestedColumn, cVal);
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

    private IntervalInfo getIntervalInfo(ArrayList<IntervalInfo> intervalInfoList, long timestamp) {
        for (IntervalInfo intervalInfo : intervalInfoList) {
            if (timestamp >= intervalInfo.getTimeLowerBound() && timestamp < intervalInfo.getTimeUpperBound()) {
                return intervalInfo;
            }
        }
        return null;
    }

    private void handleFlush() {
        if (fileManager.isWriting()) {
            fileManager.flush();
        }
    }
}
