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
            // Stage1: write
            tsdbEngineSample.connect();

            Map<String, ColumnValue> columns = new HashMap<>();
            ByteBuffer buffer = ByteBuffer.allocate(3);
            buffer.put((byte) 70);
            buffer.put((byte) 71);
            buffer.put((byte) 72);
            columns.put("col1", new ColumnValue.IntegerColumn(123));
            columns.put("col2", new ColumnValue.DoubleFloatColumn(1.23));
            columns.put("col3", new ColumnValue.StringColumn(buffer));
            String str = "12345678912345678";
            ArrayList<Row> rowList = new ArrayList<>();
            rowList.add(new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), 1, columns));

            tsdbEngineSample.createTable("test", null);
            tsdbEngineSample.upsert(new WriteRequest("test", rowList));

            tsdbEngineSample.shutdown();

            // Stage2: read
            tsdbEngineSample.connect();

            ArrayList<Vin> vinList = new ArrayList<>();
            vinList.add(new Vin(str.getBytes(StandardCharsets.UTF_8)));
            Set<String> requestedColumns = new HashSet<>(Arrays.asList("col1", "col2", "col3"));
            ArrayList<Row> resultSet = tsdbEngineSample.executeLatestQuery(new LatestQueryRequest("test", vinList, requestedColumns));
            showResult(resultSet);

            tsdbEngineSample.shutdown();

            // Stage3: overwrite
            tsdbEngineSample.connect();

            buffer.flip();
            columns = new HashMap<>();
            columns.put("col1", new ColumnValue.IntegerColumn(321));
            columns.put("col2", new ColumnValue.DoubleFloatColumn(1.23));
            columns.put("col3", new ColumnValue.StringColumn(buffer));
            str = "12345678912345678";
            rowList = new ArrayList<>();
            rowList.add(new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), 1, columns));
            str = "98765432123456789";
            rowList.add(new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), 1, columns));
            rowList.add(new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), 2, columns));
            rowList.add(new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), 3, columns));
            vinList.add(new Vin(str.getBytes(StandardCharsets.UTF_8)));

            tsdbEngineSample.upsert(new WriteRequest("test", rowList));
            resultSet = tsdbEngineSample.executeLatestQuery(new LatestQueryRequest("test", vinList, requestedColumns));
            showResult(resultSet);
            resultSet = tsdbEngineSample.executeTimeRangeQuery(new TimeRangeQueryRequest("test", new Vin(str.getBytes(StandardCharsets.UTF_8)), requestedColumns, 1, 3));
            showResult(resultSet);


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