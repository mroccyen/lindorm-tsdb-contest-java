package com.alibaba.lindorm.contest.impl;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class IndexLoaderTask extends Thread {
    private final BlockingQueue<IndexLoadCompleteNotice> writeRequestQueue = new ArrayBlockingQueue<>(50);

    private boolean stop = false;

    private IndexLoadCompleteWrapper indexLoadCompleteWrapper;

    private long size;

    public void shutdown() {
        stop = true;
    }

    public BlockingQueue<IndexLoadCompleteNotice> getWriteRequestQueue() {
        return writeRequestQueue;
    }

    public void waitComplete(IndexLoadCompleteWrapper wrapper) {
        indexLoadCompleteWrapper = wrapper;
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                IndexLoadCompleteNotice notice = writeRequestQueue.poll(5, TimeUnit.MILLISECONDS);
                if (notice != null) {
                    if (notice.isComplete()) {
                        indexLoadCompleteWrapper.getLock().lock();
                        indexLoadCompleteWrapper.getCondition().signal();
                        indexLoadCompleteWrapper.getLock().unlock();
                        //System.out.println(">>> IndexResolveTask load index data size: " + size);
                    } else {
                        size++;
                        Index index = new Index();
                        index.setOffset(notice.getOffset());
                        index.setRowKey(notice.getVin());
                        IndexLoader.offerIndex(notice.getTableName(), notice.getTimestamp(), index);
                    }
                }
            } catch (Exception e) {
                System.out.println(">>> " + Thread.currentThread().getName() + " thread happen exception: " + e.getMessage());
                for (StackTraceElement stackTraceElement : e.getStackTrace()) {
                    System.out.println(">>> " + Thread.currentThread().getName() + " thread happen exception: " + stackTraceElement.toString());
                }
                System.exit(-1);
            }
        }
    }
}