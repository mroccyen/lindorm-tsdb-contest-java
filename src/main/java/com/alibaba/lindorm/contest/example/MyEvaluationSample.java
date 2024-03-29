//
// A simple evaluation program example helping you to understand how the
// evaluation program calls the protocols you will implement.
// Formal evaluation program is much more complex than this.
//

/*
 * Copyright Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.lindorm.contest.example;

import com.alibaba.lindorm.contest.TSDBEngineImpl;
import com.alibaba.lindorm.contest.structs.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * This is an evaluation program sample.
 * The evaluation program will create a new Database using the targeted
 * local disk path, then write several rows, then check the correctness
 * of the written data, and then run the read test.
 * <p>
 * The actual evaluation program is far more complex than the sample, e.g.,
 * it might contain the restarting progress to clean all memory cache, it
 * might test the memory cache strategies by a pre-warming procedure, and
 * it might perform read and write tests crosswise, or even concurrently.
 * Besides, as long as you write to the interface specification, you don't
 * have to worry about incompatibility with our evaluation program.
 */
public class MyEvaluationSample {
    public static void main(String[] args) throws IOException {

        File dataDir = new File("/Users/qingshanpeng/Desktop/ali-test");
        if (dataDir.isFile()) {
            throw new IllegalStateException("Clean the directory before we start the demo");
        }
        if (!dataDir.isDirectory()) {
            boolean ret = dataDir.mkdirs();
            if (!ret) {
                throw new IllegalStateException("Cannot create the temp data directory: " + dataDir);
            }
        }
        TSDBEngineImpl tsdbEngineSample = new TSDBEngineImpl(dataDir);

        try {
            Map<String, ColumnValue.ColumnType> cols = new HashMap<>();
            cols.put("col1", ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
            cols.put("col2", ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
            cols.put("col3", ColumnValue.ColumnType.COLUMN_TYPE_STRING);
            Schema schema = new Schema(cols);

            // Stage1: write
            tsdbEngineSample.connect();
            tsdbEngineSample.createTable("test", schema);

//            ByteBuffer buffer = ByteBuffer.allocate(3);
//            buffer.put((byte) 70);
//            buffer.put((byte) 71);
//            buffer.put((byte) 72);
            String s = ">>> executeTimeRangeQuery happen exception: java.io.EOFException>>> executeTimeRangeQuery happen exception: com.alibaba.lindorm.contest.impl.store.ByteBuffersDataInput.seek(ByteBuffersDataInput.java:393)>>> executeTimeRangeQuery happen exception: com.alibaba.lindorm.contest.impl.query.DataQueryHandler.executeTimeRangeQuery(DataQueryHandler.java:116)>>> executeTimeRangeQuery happen exception: com.alibaba.lindorm.contest.impl.query.DataQueryHandler.executeTimeRangeQuery(DataQueryHandler.java:53)>>> executeTimeRangeQuery happen exception: com.alibaba.lindorm.contest.TSDBEngineImpl.executeTimeRangeQuery(TSDBEngineImpl.java:102)>>> executeTimeRangeQuery happen exception: com.alibaba.lindorm.contest.adapter.JavaEngineAdapter.executeTimeRangeQuery0(JavaEngineAdapter.java:61)>>> executeTimeRangeQuery happen exception: com.alibaba.lindorm.contest.adapter.EngineAdapter.executeTimeRangeQuery(EngineAdapter.java:72)>>> executeTimeRangeQuery happen exception: com.alibaba.lindorm.contest.execute.write.DataWriter.pickupCheckWrittenData(DataWriter.java:93)>>> executeTimeRangeQuery happen exception: com.alibaba.lindorm.contest.execute.write.DataWriter.run(DataWriter.java:64)>>> executeTimeRangeQuery happen exception: java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)>>> executeTimeRangeQuery happen exception: java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)>>> executeTimeRangeQuery happen exception: java.base/java.lang.Thread.run(Thread.java:830)";
            ByteBuffer buffer = ByteBuffer.allocate(1544);
            buffer.put(s.getBytes());
            buffer.flip();
            Map<String, ColumnValue> columns1 = new HashMap<>();
            columns1.put("col1", new ColumnValue.IntegerColumn(123));
            columns1.put("col2", new ColumnValue.DoubleFloatColumn(1.23));
            columns1.put("col3", new ColumnValue.StringColumn(buffer));
            String str = "12345678912345678";
            ArrayList<Row> rowList = new ArrayList<>();

            rowList.add(new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), 1689091341, columns1));
            tsdbEngineSample.upsert(new WriteRequest("test", rowList));

            //read
            ArrayList<Vin> vinList1 = new ArrayList<>();
            vinList1.add(new Vin(str.getBytes(StandardCharsets.UTF_8)));
            Set<String> requestedColumns1 = new HashSet<>(Arrays.asList("col1", "col2", "col3"));
            ArrayList<Row> resultSet1 = tsdbEngineSample.executeLatestQuery(new LatestQueryRequest("test", vinList1, requestedColumns1));
            showResult(resultSet1);

            tsdbEngineSample.shutdown();

            System.out.println("-------------------------------------------------------------------------------------------------------------------");

            // Stage2: read
            tsdbEngineSample.connect();

            ArrayList<Vin> vinList2 = new ArrayList<>();
            vinList2.add(new Vin(str.getBytes(StandardCharsets.UTF_8)));
            Set<String> requestedColumns2 = new HashSet<>(Arrays.asList("col1", "col2", "col3"));
            ArrayList<Row> resultSet2 = tsdbEngineSample.executeLatestQuery(new LatestQueryRequest("test", vinList2, requestedColumns2));
            showResult(resultSet2);

            tsdbEngineSample.shutdown();

            System.out.println("-------------------------------------------------------------------------------------------------------------------");

            // Stage3: overwrite
            tsdbEngineSample.connect();

            Map<String, ColumnValue> columns = new HashMap<>();
            columns.put("col1", new ColumnValue.IntegerColumn(321));
            columns.put("col2", new ColumnValue.DoubleFloatColumn(1.23));
            columns.put("col3", new ColumnValue.StringColumn(buffer));
            String str1 = "12345678912345678";
            rowList = new ArrayList<>();
            rowList.add(new Row(new Vin(str1.getBytes(StandardCharsets.UTF_8)), 1689091340, columns));
            String str2 = "98765432123456789";
            rowList.add(new Row(new Vin(str2.getBytes(StandardCharsets.UTF_8)), 1689091341, columns));
            rowList.add(new Row(new Vin(str2.getBytes(StandardCharsets.UTF_8)), 1689091342, columns));
            rowList.add(new Row(new Vin(str2.getBytes(StandardCharsets.UTF_8)), 1689091343, columns));

            ArrayList<Vin> vinList3 = new ArrayList<>();
            vinList3.add(new Vin(str1.getBytes(StandardCharsets.UTF_8)));
            vinList3.add(new Vin(str2.getBytes(StandardCharsets.UTF_8)));
            Set<String> requestedColumns3 = new HashSet<>(Arrays.asList("col1", "col2", "col3"));

            tsdbEngineSample.upsert(new WriteRequest("test", rowList));
            ArrayList<Row> resultSet3 = tsdbEngineSample.executeLatestQuery(new LatestQueryRequest("test", vinList3, requestedColumns3));
            showResult(resultSet3);
            resultSet3 = tsdbEngineSample.executeTimeRangeQuery(new TimeRangeQueryRequest("test", new Vin(str2.getBytes(StandardCharsets.UTF_8)), requestedColumns3, 1689091341, 1689091343));
            showResult(resultSet3);

            tsdbEngineSample.shutdown();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void showResult(ArrayList<Row> resultSet) {
        for (Row result : resultSet) {
            System.out.println(result);
        }
        System.out.println("-------next query-------");
    }
}