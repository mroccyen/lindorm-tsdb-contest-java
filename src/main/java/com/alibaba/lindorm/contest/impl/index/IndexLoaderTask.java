package com.alibaba.lindorm.contest.impl.index;

import com.alibaba.lindorm.contest.structs.Vin;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class IndexLoaderTask extends Thread {
    private final BlockingQueue<IndexLoadCompleteNotice> writeRequestQueue = new ArrayBlockingQueue<>(50);

    private boolean stop = false;

    private IndexLoadCompleteWrapper indexLoadCompleteWrapper;

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
                    } else {
                        Index index = new Index();
                        index.setOffset(notice.getOffset());
                        index.setRowKey(notice.getVin());
                        index.setLatestTimestamp(notice.getTimestamp());
                        IndexLoader.offerLatestIndex(notice.getTableName(), new Vin(notice.getVin()), index);
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