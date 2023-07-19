//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.impl.WriteRequestWrapper;
import com.alibaba.lindorm.contest.impl.HandleRequestTask;
import com.alibaba.lindorm.contest.structs.LatestQueryRequest;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Schema;
import com.alibaba.lindorm.contest.structs.TimeRangeQueryRequest;
import com.alibaba.lindorm.contest.structs.WriteRequest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class TSDBEngineImpl extends TSDBEngine {

    private final static ThreadLocal<WriteRequestWrapper> WRITE_REQUEST_THREAD_LOCAL = ThreadLocal.withInitial(WriteRequestWrapper::new);

    private HandleRequestTask writeTask;

    private HashMap<String, Schema> tableSchema = new HashMap<>();

    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    public TSDBEngineImpl(File dataPath) {
        super(dataPath);
    }

    @Override
    public void connect() throws IOException {
        writeTask = new HandleRequestTask(getDataPath());
        //开启写入任务
        writeTask.start();
    }

    @Override
    public void createTable(String tableName, Schema schema) throws IOException {
        //缓存表元数据信息
        tableSchema.put(tableName, schema);
    }

    @Override
    public void shutdown() {

    }

    @Override
    public void upsert(WriteRequest wReq) throws IOException {
        WriteRequestWrapper writeRequestWrapper = WRITE_REQUEST_THREAD_LOCAL.get();
        writeRequestWrapper.bindRequest(wReq);
        writeRequestWrapper.bingSchema(selectSchema(wReq.getTableName()));
        writeRequestWrapper.getLock().lock();
        writeTask.receiveRequest(writeRequestWrapper);
        try {
            //等待force落盘后进行通知
            writeRequestWrapper.getCondition().await();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        writeRequestWrapper.getLock().unlock();
    }

    @Override
    public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        return null;
    }

    @Override
    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        return null;
    }

    private Schema selectSchema(String tableName) {
        return tableSchema.get(tableName);
    }
}
