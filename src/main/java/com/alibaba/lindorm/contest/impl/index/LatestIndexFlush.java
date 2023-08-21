package com.alibaba.lindorm.contest.impl.index;

import com.alibaba.lindorm.contest.impl.file.FileManager;
import com.alibaba.lindorm.contest.structs.Vin;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LatestIndexFlush {
    private final FileManager fileManager;

    public LatestIndexFlush(FileManager fileManager) {
        this.fileManager = fileManager;
    }

    public void flushLatestIndex() {
        try {
            //每个线程有10M缓冲区用于写数据
            ByteBuffer dataWriteByteBuffer = ByteBuffer.allocate(33);
            ConcurrentHashMap<String, ConcurrentHashMap<Vin, Index>> latestIndexCacheMap = IndexLoader.getLatestIndexCacheMap();
            for (Map.Entry<String, ConcurrentHashMap<Vin, Index>> entry : latestIndexCacheMap.entrySet()) {
                FileChannel latestIndexFileChannel = fileManager.getWriteLatestIndexFile(entry.getKey());
                for (Map.Entry<Vin, Index> e : entry.getValue().entrySet()) {
                    dataWriteByteBuffer.put(e.getValue().getRowKey());
                    dataWriteByteBuffer.putLong(e.getValue().getOffset());
                    dataWriteByteBuffer.putLong(e.getValue().getLatestTimestamp());
                    dataWriteByteBuffer.flip();
                    latestIndexFileChannel.write(dataWriteByteBuffer);
                    dataWriteByteBuffer.clear();
                }
                latestIndexFileChannel.force(false);
            }
        } catch (Exception e) {
            System.out.println(">>> " + Thread.currentThread().getName() + " LatestIndexFlush happen exception: " + e.getMessage());
            for (StackTraceElement stackTraceElement : e.getStackTrace()) {
                System.out.println(">>> " + Thread.currentThread().getName() + " LatestIndexFlush happen exception: " + stackTraceElement.toString());
            }
        }
    }
}
