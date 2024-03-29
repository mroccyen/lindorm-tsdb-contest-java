package com.alibaba.lindorm.contest.impl.wtire;

import com.alibaba.lindorm.contest.impl.common.CommonSetting;
import com.alibaba.lindorm.contest.impl.file.FileManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class HandleRequestTask extends Thread {
    private final BlockingQueue<WriteRequestWrapper> writeRequestQueue = new ArrayBlockingQueue<>(100);

    private final BlockingQueue<List<WriteRequestWrapper>> flushRequestQueue = new ArrayBlockingQueue<>(400);

    private final List<FlushRequestTask> flushRequestTaskList = new ArrayList<>();

    private boolean stop = false;

    public HandleRequestTask(FileManager fileManager) {
        for (int i = 0; i < CommonSetting.WRITE_THREAD_COUNT; i++) {
            FlushRequestTask flushRequestTask = new FlushRequestTask(fileManager, this);
            flushRequestTaskList.add(flushRequestTask);
            flushRequestTask.setName("FlushRequestTask-" + i);
            flushRequestTask.start();
        }
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
        while (!stop) {
            try {
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
                boolean offered = flushRequestQueue.offer(writeRequestWrapperList);
                if (!offered) {
                    System.out.println("入对失败");
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

    public void shutdown() {
        while (writeRequestQueue.size() > 0) {
        }
        while (flushRequestQueue.size() > 0) {

        }
        for (FlushRequestTask flushRequestTask : flushRequestTaskList) {
            flushRequestTask.shutdown();
        }
        stop = true;
    }

    public BlockingQueue<List<WriteRequestWrapper>> getFlushRequestQueue() {
        return flushRequestQueue;
    }
}
