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
            String tableName = "test";
            String vinName = "12345678912345678";
            String col1 = "col1";
            String col2 = "col2";
            String col3 = "col3";

            Map<String, ColumnValue.ColumnType> cols = new HashMap<>();
            cols.put(col1, ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
            cols.put(col2, ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
            cols.put(col3, ColumnValue.ColumnType.COLUMN_TYPE_STRING);
            Schema schema = new Schema(cols);

            Set<String> requestedColumns = new HashSet<>(Arrays.asList(col1, col2, col3));
            ArrayList<Vin> vinList = new ArrayList<>();
            vinList.add(new Vin(vinName.getBytes(StandardCharsets.UTF_8)));

            // Stage1: write
            tsdbEngineSample.connect();
            tsdbEngineSample.createTable(tableName, schema);

            ByteBuffer buffer = ByteBuffer.allocate(3);
            buffer.put((byte) 70);
            buffer.put((byte) 71);
            buffer.put((byte) 72);
            buffer.flip();

            ArrayList<Row> rowList = new ArrayList<>();
            Map<String, ColumnValue> columns1 = new HashMap<>();
            columns1.put(col1, new ColumnValue.IntegerColumn(12));
            columns1.put(col2, new ColumnValue.DoubleFloatColumn(1.23));
            columns1.put(col3, new ColumnValue.StringColumn(buffer));
            rowList.add(new Row(new Vin(vinName.getBytes(StandardCharsets.UTF_8)), 1689091341000L, columns1));

            columns1 = new HashMap<>();
            columns1.put(col1, new ColumnValue.IntegerColumn(15));
            columns1.put(col2, new ColumnValue.DoubleFloatColumn(1.73));
            columns1.put(col3, new ColumnValue.StringColumn(buffer));
            rowList.add(new Row(new Vin(vinName.getBytes(StandardCharsets.UTF_8)), 1689091342000L, columns1));

            columns1 = new HashMap<>();
            columns1.put(col1, new ColumnValue.IntegerColumn(10));
            columns1.put(col2, new ColumnValue.DoubleFloatColumn(2.23));
            columns1.put(col3, new ColumnValue.StringColumn(buffer));
            rowList.add(new Row(new Vin(vinName.getBytes(StandardCharsets.UTF_8)), 1689091343000L, columns1));

            columns1 = new HashMap<>();
            columns1.put(col1, new ColumnValue.IntegerColumn(8));
            columns1.put(col2, new ColumnValue.DoubleFloatColumn(1.20));
            columns1.put(col3, new ColumnValue.StringColumn(buffer));
            rowList.add(new Row(new Vin(vinName.getBytes(StandardCharsets.UTF_8)), 1689091344000L, columns1));

            columns1 = new HashMap<>();
            columns1.put(col1, new ColumnValue.IntegerColumn(1));
            columns1.put(col2, new ColumnValue.DoubleFloatColumn(0.23));
            columns1.put(col3, new ColumnValue.StringColumn(buffer));
            rowList.add(new Row(new Vin(vinName.getBytes(StandardCharsets.UTF_8)), 1689091345000L, columns1));

            columns1 = new HashMap<>();
            columns1.put(col1, new ColumnValue.IntegerColumn(30));
            columns1.put(col2, new ColumnValue.DoubleFloatColumn(3.23));
            columns1.put(col3, new ColumnValue.StringColumn(buffer));
            rowList.add(new Row(new Vin(vinName.getBytes(StandardCharsets.UTF_8)), 1689091346000L, columns1));

            columns1 = new HashMap<>();
            columns1.put(col1, new ColumnValue.IntegerColumn(22));
            columns1.put(col2, new ColumnValue.DoubleFloatColumn(1.83));
            columns1.put(col3, new ColumnValue.StringColumn(buffer));
            rowList.add(new Row(new Vin(vinName.getBytes(StandardCharsets.UTF_8)), 1689091347000L, columns1));

            tsdbEngineSample.write(new WriteRequest(tableName, rowList));

            //read
            System.out.println("executeLatestQuery-------------------------------------------------------------------------------------------------------------------");
            ArrayList<Row> resultSet1 = tsdbEngineSample.executeLatestQuery(new LatestQueryRequest(tableName, vinList, requestedColumns));
            showResult(resultSet1);
            System.out.println("executeLatestQuery-------------------------------------------------------------------------------------------------------------------");

            tsdbEngineSample.shutdown();

            // Stage2: read
            tsdbEngineSample.connect();

            System.out.println("executeLatestQuery-------------------------------------------------------------------------------------------------------------------");
            ArrayList<Row> resultSet2 = tsdbEngineSample.executeLatestQuery(new LatestQueryRequest(tableName, vinList, requestedColumns));
            showResult(resultSet2);
            System.out.println("executeLatestQuery-------------------------------------------------------------------------------------------------------------------");

            tsdbEngineSample.shutdown();

            // Stage3: read
            tsdbEngineSample.connect();

            System.out.println("executeLatestQuery-------------------------------------------------------------------------------------------------------------------");
            ArrayList<Row> resultSet3 = tsdbEngineSample.executeLatestQuery(new LatestQueryRequest(tableName, vinList, requestedColumns));
            showResult(resultSet3);
            System.out.println("executeLatestQuery-------------------------------------------------------------------------------------------------------------------");

            System.out.println("TimeRangeQueryRequest-------------------------------------------------------------------------------------------------------------------");
            resultSet3 = tsdbEngineSample.executeTimeRangeQuery(new TimeRangeQueryRequest(tableName, new Vin(vinName.getBytes(StandardCharsets.UTF_8)), requestedColumns,
                1689091341000L, 1689091343000L));
            showResult(resultSet3);
            System.out.println("TimeRangeQueryRequest-------------------------------------------------------------------------------------------------------------------");

            System.out.println("TimeRangeAggregationRequest-------------------------------------------------------------------------------------------------------------------");
            resultSet3 = tsdbEngineSample.executeAggregateQuery(new TimeRangeAggregationRequest(tableName, new Vin(vinName.getBytes(StandardCharsets.UTF_8)), col1,
                1689091341056L, 1689091347056L, Aggregator.AVG));
            showResult(resultSet3);
            System.out.println("TimeRangeAggregationRequest-------------------------------------------------------------------------------------------------------------------");

            System.out.println("TimeRangeDownsampleRequest-------------------------------------------------------------------------------------------------------------------");
            resultSet3 = tsdbEngineSample.executeDownsampleQuery(new TimeRangeDownsampleRequest(tableName, new Vin(vinName.getBytes(StandardCharsets.UTF_8)), col1,
                1689091341056L, 1689091347056L, Aggregator.AVG, 2000L, new CompareExpression(new ColumnValue.IntegerColumn(2), CompareExpression.CompareOp.GREATER)));
            showResult(resultSet3);
            System.out.println("TimeRangeDownsampleRequest-------------------------------------------------------------------------------------------------------------------");

            tsdbEngineSample.shutdown();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void showResult(ArrayList<Row> resultSet) {
        for (Row result : resultSet) {
            System.out.println(result);
        }
    }
}