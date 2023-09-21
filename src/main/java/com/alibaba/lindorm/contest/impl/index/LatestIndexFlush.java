package com.alibaba.lindorm.contest.impl.index;

import com.alibaba.lindorm.contest.impl.file.FileManager;
import com.alibaba.lindorm.contest.impl.store.ByteBuffersDataOutput;
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
            ConcurrentHashMap<String, ConcurrentHashMap<Vin, Index>> latestIndexCacheMap = IndexLoader.getLatestIndexCacheMap();
            for (Map.Entry<String, ConcurrentHashMap<Vin, Index>> entry : latestIndexCacheMap.entrySet()) {
                ByteBuffersDataOutput byteBuffersDataOutput = new ByteBuffersDataOutput();
                FileChannel latestIndexFileChannel = fileManager.getWriteLatestIndexFile(entry.getKey());
                for (Map.Entry<Vin, Index> e : entry.getValue().entrySet()) {
                    byteBuffersDataOutput.writeBytes(e.getValue().getRowKey());
                    byteBuffersDataOutput.writeVLong(e.getValue().getTimestamp());
                    byteBuffersDataOutput.writeVInt(e.getValue().getBytes().length);
                    byteBuffersDataOutput.writeBytes(e.getValue().getBuffer());
                    ByteBuffer totalByte = ByteBuffer.allocate((int) byteBuffersDataOutput.size());
                    for (int i = 0; i < byteBuffersDataOutput.toWriteableBufferList().size(); i++) {
                        totalByte.put(byteBuffersDataOutput.toWriteableBufferList().get(i));
                    }
                    totalByte.flip();
                    latestIndexFileChannel.write(totalByte);
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
