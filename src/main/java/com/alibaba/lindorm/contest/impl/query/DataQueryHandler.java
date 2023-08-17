package com.alibaba.lindorm.contest.impl.query;

import com.alibaba.lindorm.contest.impl.compress.DeflaterUtils;
import com.alibaba.lindorm.contest.impl.file.FileManager;
import com.alibaba.lindorm.contest.impl.index.Index;
import com.alibaba.lindorm.contest.impl.index.IndexLoader;
import com.alibaba.lindorm.contest.impl.schema.SchemaMeta;
import com.alibaba.lindorm.contest.impl.store.ByteBuffersDataInput;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.LatestQueryRequest;
import com.alibaba.lindorm.contest.structs.Row;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
            Index latestIndex = IndexLoader.getLatestIndex(tableName, vin);
            if (latestIndex == null) {
                return null;
            }
            fileChannel.read(sizeByteBuffer, latestIndex.getOffset());
            sizeByteBuffer.flip();
            ByteBuffersDataInput dataInput = new ByteBuffersDataInput(Collections.singletonList(sizeByteBuffer));

            long t = dataInput.readVLong();
            long size = dataInput.readVLong();
            ByteBuffer tempBuffer = ByteBuffer.allocate((int) size);
            dataInput.readBytes(tempBuffer, (int) size);
            byte[] unzipBytes = DeflaterUtils.unzipString(tempBuffer.array());
            dataInput = new ByteBuffersDataInput(Collections.singletonList(ByteBuffer.wrap(unzipBytes)));

            Map<String, ColumnValue> columns = new HashMap<>();
            ArrayList<String> integerColumnsNameList = schemaMeta.getIntegerColumnsName();
            for (String cName : integerColumnsNameList) {
                int intVal = dataInput.readVInt();
                ColumnValue cVal = new ColumnValue.IntegerColumn(intVal);
                if (requestedColumns.contains(cName)) {
                    columns.put(cName, cVal);
                }
            }
            ArrayList<String> doubleColumnsNameList = schemaMeta.getDoubleColumnsName();
            for (String cName : doubleColumnsNameList) {
                double doubleVal = dataInput.readZDouble();
                ColumnValue cVal = new ColumnValue.DoubleFloatColumn(doubleVal);
                if (requestedColumns.contains(cName)) {
                    columns.put(cName, cVal);
                }
            }
            ArrayList<String> stringColumnsNameList = schemaMeta.getStringColumnsName();
            List<Integer> stringLengthList = new ArrayList<>();
            for (String cName : stringColumnsNameList) {
                int length = dataInput.readVInt();
                stringLengthList.add(length);
            }
            String s = dataInput.readString();
            ByteBuffer buffer = ByteBuffer.wrap(s.getBytes());
            for (int i = 0; i < stringLengthList.size(); i++) {
                int length = stringLengthList.get(i);
                byte[] bytes = new byte[length];
                for (int j = 0; j < length; j++) {
                    bytes[j] = buffer.get();
                }
                String cName = stringColumnsNameList.get(i);
                ColumnValue cVal = new ColumnValue.StringColumn(ByteBuffer.wrap(bytes));
                if (requestedColumns.contains(cName)) {
                    columns.put(cName, cVal);
                }
            }
            if (latestTimestamp.get(vin) < t) {
                //构建Row
                latestRowMap.put(vin, new Row(vin, t, columns));
                latestTimestamp.put(vin, t);
            }
            sizeByteBuffer.clear();
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
                List<Integer> stringLengthList = new ArrayList<>();
                for (String cName : stringColumnsNameList) {
                    int length = tempDataInput.readVInt();
                    stringLengthList.add(length);
                }
                String s = tempDataInput.readString();
                ByteBuffer buffer = ByteBuffer.wrap(s.getBytes());
                for (int i = 0; i < stringLengthList.size(); i++) {
                    int length = stringLengthList.get(i);
                    byte[] bytes = new byte[length];
                    for (int j = 0; j < length; j++) {
                        bytes[j] = buffer.get();
                    }
                    String cName = stringColumnsNameList.get(i);
                    ColumnValue cVal = new ColumnValue.StringColumn(ByteBuffer.wrap(bytes));
                    if (requestedColumns.contains(cName)) {
                        columns.put(cName, cVal);
                    }
                }

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
}
