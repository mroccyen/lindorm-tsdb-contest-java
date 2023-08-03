//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.impl.*;
import com.alibaba.lindorm.contest.structs.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class TSDBEngineImpl extends TSDBEngine {
    private final static ThreadLocal<WriteRequestWrapper> WRITE_REQUEST_THREAD_LOCAL = ThreadLocal.withInitial(WriteRequestWrapper::new);
    private HandleRequestTask writeTask;
    private IndexLoaderTask indexLoaderTask;
    private DataQueryHandler dataQueryHandler;
    private FileManager fileManager;
    private SchemaHandler schemaHandler;

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
        //初始化文件管理
        fileManager = new FileManager(getDataPath());
        fileManager.loadExistFile();
        //表信息处理
        schemaHandler = new SchemaHandler(fileManager, getDataPath());
        schemaHandler.loadTableInfo();
        //加载索引信息
        //indexLoaderTask = new IndexLoaderTask();
        //indexLoaderTask.setName("IndexLoaderTask");
        //indexLoaderTask.start();
        //IndexLoader.loadIndex(fileManager, indexLoaderTask);
        //开启写入任务
        writeTask = new HandleRequestTask(fileManager);
        writeTask.setName("HandleRequestTask");
        writeTask.start();
        //初始化数据查询处理器
        dataQueryHandler = new DataQueryHandler(fileManager);
        System.out.println(">>> connect complete");
    }

    @Override
    public void createTable(String tableName, Schema schema) throws IOException {
        schemaHandler.cacheTableInfo(tableName, schema);
        fileManager.initTableWriteLockMap(tableName);
        System.out.println(">>> createTable complete");
    }

    @Override
    public void shutdown() {
        writeTask.shutdown();
        //indexLoaderTask.shutdown();
        //IndexLoader.shutdown();
        fileManager.shutdown();
        schemaHandler.flushTableInfo();
        System.out.println(">>> shutdown complete");
    }

    @Override
    public void upsert(WriteRequest wReq) throws IOException {
        WriteRequestWrapper writeRequestWrapper = WRITE_REQUEST_THREAD_LOCAL.get();
        writeRequestWrapper.bindRequest(wReq);
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
        return dataQueryHandler.executeLatestQuery(pReadReq);
    }

    @Override
    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        return dataQueryHandler.executeTimeRangeQuery(trReadReq);
    }
}
