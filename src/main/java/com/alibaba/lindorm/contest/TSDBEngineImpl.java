//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.impl.*;
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

    private final HashMap<String, Schema> tableSchema = new HashMap<>();

    private QueryHandler queryHandler;

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
        String absolutePath = dataPath.getAbsolutePath();
        String dp = absolutePath + File.separator + CommonSetting.DATA_NAME;
        File dpFile = new File(dp);
        if (!dpFile.exists()) {
            dpFile.createNewFile();
        }
        String ip = absolutePath + File.separator + CommonSetting.INDEX_NAME;
        File ipFile = new File(ip);
        if (!ipFile.exists()) {
            ipFile.createNewFile();
        }
        writeTask = new HandleRequestTask(dpFile, ipFile);
        //开启写入任务
        writeTask.start();
        //加载索引信息
        IndexBufferHandler.initIndexBuffer(ipFile);
        //初始化数据查询处理器
        queryHandler = new QueryHandler(dpFile);
    }

    @Override
    public void createTable(String tableName, Schema schema) throws IOException {
        //缓存表元数据信息
        tableSchema.put(tableName, schema);
    }

    @Override
    public void shutdown() {
        IndexBufferHandler.shutdown();
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
            System.out.println(e.getMessage());
            System.exit(-1);
        }
        writeRequestWrapper.getLock().unlock();
    }

    @Override
    public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        return queryHandler.executeLatestQuery(pReadReq);
    }

    @Override
    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        return queryHandler.executeTimeRangeQuery(trReadReq);
    }

    private Schema selectSchema(String tableName) {
        return tableSchema.get(tableName);
    }
}
