package com.alibaba.lindorm.contest.impl;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardOpenOption.WRITE;

public class FlushRequestTask extends Thread {
    private final BlockingQueue<List<WriteRequestWrapper>> flushRequestQueue = new ArrayBlockingQueue<>(Integer.MAX_VALUE);

    private final FileChannel writeFileChanel;

    public FlushRequestTask(File dataPath) throws IOException {
        writeFileChanel = FileChannel.open(dataPath.toPath(), WRITE);
    }

    public BlockingQueue<List<WriteRequestWrapper>> getFlushRequestQueue() {
        return flushRequestQueue;
    }

    @Override
    public void run() {
        try {
            while (true) {
                List<WriteRequestWrapper> writeRequestWrapperList = flushRequestQueue.poll(5, TimeUnit.MILLISECONDS);
                if (writeRequestWrapperList != null && writeRequestWrapperList.size() > 0) {
                    //执行刷盘操作
                    doWrite(writeRequestWrapperList);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private void doWrite(List<WriteRequestWrapper> writeRequestWrapperList) {

    }
}
