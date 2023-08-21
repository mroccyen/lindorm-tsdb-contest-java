package com.alibaba.lindorm.contest.impl.query;

import com.alibaba.lindorm.contest.impl.compress.DeflaterUtils;
import com.alibaba.lindorm.contest.impl.file.FileManager;
import com.alibaba.lindorm.contest.impl.index.Index;
import com.alibaba.lindorm.contest.impl.index.IndexLoader;
import com.alibaba.lindorm.contest.impl.schema.SchemaMeta;
import com.alibaba.lindorm.contest.impl.store.ByteBuffersDataInput;
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
            System.out.println(">>> executeLatestQuery happen exception: " + ex.getClass().getName());
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
            System.out.println(">>> executeTimeRangeQuery happen exception: " + ex.getClass().getName());
            for (StackTraceElement stackTraceElement : ex.getStackTrace()) {
                System.out.println(">>> executeTimeRangeQuery happen exception: " + stackTraceElement.toString());
            }
            throw new IOException(ex);
        }
        return result;
    }

    private ArrayList<Row> executeLatestQuery(String tableName, Collection<Vin> vinList, Set<String> requestedColumns) throws IOException {
        SchemaMeta schemaMeta = fileManager.getSchemaMeta(tableName);

        Map<Vin, Row> latestRowMap = new HashMap<>();
        for (Vin vin : vinList) {
            Index latestIndex = IndexLoader.getLatestIndex(tableName, vin);
            if (latestIndex == null) {
                continue;
            }
            byte[] unzipBytes = DeflaterUtils.unzipString(latestIndex.getBuffer().array());
            ByteBuffersDataInput tempDataInput = new ByteBuffersDataInput(Collections.singletonList(ByteBuffer.wrap(unzipBytes)));

            Map<String, ColumnValue> columns = getColumns(schemaMeta, tempDataInput, requestedColumns);
            //构建Row
            latestRowMap.put(vin, new Row(vin, latestIndex.getLatestTimestamp(), columns));
        }
        return new ArrayList<>(latestRowMap.values());
    }

    private ArrayList<Row> executeTimeRangeQuery(String tableName, Vin vin, Set<String> requestedColumns, long timeLowerBound, long timeUpperBound) throws IOException {
        SchemaMeta schemaMeta = fileManager.getSchemaMeta(tableName);
        ArrayList<Row> rowList = new ArrayList<>();
        Map<Vin, ArrayList<Row>> timeRangeRowMap = new HashMap<>();

        FileChannel fileChannel = fileManager.getReadFileChannel(tableName, vin);
        if (fileChannel == null || fileChannel.size() == 0) {
            return new ArrayList<>();
        }
        MappedByteBuffer sizeByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        ByteBuffersDataInput dataInput = new ByteBuffersDataInput(Collections.singletonList(sizeByteBuffer));
        while (dataInput.position() < dataInput.size()) {
            long t = dataInput.readVLong();
            long size = dataInput.readVLong();
            long position = dataInput.position() + size;
            if (t >= timeLowerBound && t < timeUpperBound) {
                ByteBuffer tempBuffer = ByteBuffer.allocate((int) size);
                dataInput.readBytes(tempBuffer, (int) size);
                byte[] unzipBytes = DeflaterUtils.unzipString(tempBuffer.array());
                ByteBuffersDataInput tempDataInput = new ByteBuffersDataInput(Collections.singletonList(ByteBuffer.wrap(unzipBytes)));

                Map<String, ColumnValue> columns = getColumns(schemaMeta, tempDataInput, requestedColumns);

                ArrayList<Row> rows = timeRangeRowMap.get(vin);
                if (rows == null) {
                    rows = new ArrayList<>();
                }
                Row row = new Row(vin, t, columns);
                rows.add(row);
                timeRangeRowMap.put(vin, rows);
            } else {
                dataInput.seek(position);
            }
        }
        if (timeLowerBound != -1 && timeUpperBound != -1) {
            for (Map.Entry<Vin, ArrayList<Row>> e : timeRangeRowMap.entrySet()) {
                rowList.addAll(e.getValue());
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
}
