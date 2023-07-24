package com.alibaba.lindorm.contest.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class HandleRequestTask extends Thread {
    private final BlockingQueue<WriteRequestWrapper> writeRequestQueue = new ArrayBlockingQueue<>(50);

    private final FlushRequestTask flushRequestTask;

    private boolean stop = false;

    public HandleRequestTask(File dpFile, File ipFile) throws IOException {
        flushRequestTask = new FlushRequestTask(dpFile, ipFile);
        flushRequestTask.start();
    }

    public void receiveRequest(WriteRequestWrapper requestWrapper) {
        //入队列
        boolean offered = writeRequestQueue.offer(requestWrapper);
        if (!offered) {
            System.out.println("入对失败");
        }
    }

    @Override
    public void run() {
        try {
            while (!stop) {
                List<WriteRequestWrapper> writeRequestWrapperList = new ArrayList<>();
                while (true) {
                    WriteRequestWrapper writeRequestWrapper = writeRequestQueue.poll(5, TimeUnit.MILLISECONDS);
                    if (writeRequestWrapper != null) {
                        writeRequestWrapperList.add(writeRequestWrapper);
                    } else {
                        if (writeRequestWrapperList.size() != 0) {
                            //先处理已经堆积的请求
                            break;
                        }
                    }
                }
                //入对列交给刷盘线程进行处理
                boolean offered = flushRequestTask.getFlushRequestQueue().offer(writeRequestWrapperList);
                if (!offered) {
                    System.out.println("入对失败");
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        }
    }

    public void shutdown() {
        flushRequestTask.shutdown();
        stop = true;
    }
}
