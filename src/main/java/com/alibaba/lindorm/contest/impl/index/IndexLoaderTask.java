package com.alibaba.lindorm.contest.impl.index;

import com.alibaba.lindorm.contest.impl.file.FileManager;
import com.alibaba.lindorm.contest.impl.store.ByteBuffersDataInput;
import com.alibaba.lindorm.contest.structs.Vin;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class IndexLoaderTask extends Thread {
    private final BlockingQueue<IndexLoadCompleteNotice> writeRequestQueue = new ArrayBlockingQueue<>(50);

    private boolean stop = false;

    private IndexLoadCompleteWrapper indexLoadCompleteWrapper;

    private final FileManager fileManager;

    public IndexLoaderTask(FileManager fileManager) {
        this.fileManager = fileManager;
    }

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
        ByteBuffer sizeByteBuffer = ByteBuffer.allocate(1024 * 10);
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

                        FileChannel fileChannel = fileManager.getReadFileChannel(notice.getTableName(), new Vin(notice.getVin()));
                        if (fileChannel == null || fileChannel.size() == 0) {
                            continue;
                        }
                        fileChannel.read(sizeByteBuffer, notice.getOffset());
                        sizeByteBuffer.flip();
                        ByteBuffersDataInput dataInput = new ByteBuffersDataInput(Collections.singletonList(sizeByteBuffer));

                        long delta = dataInput.readVLong();
                        index.setDelta(delta);
                        long size = dataInput.readVInt();
                        ByteBuffer tempBuffer = ByteBuffer.allocate((int) size);
                        dataInput.readBytes(tempBuffer, (int) size);
                        tempBuffer.flip();
                        index.setBuffer(ByteBuffer.wrap(tempBuffer.array()));

                        IndexLoader.offerLatestIndex(notice.getTableName(), new Vin(notice.getVin()), index);
                        sizeByteBuffer.clear();
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