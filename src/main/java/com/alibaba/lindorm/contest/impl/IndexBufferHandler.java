package com.alibaba.lindorm.contest.impl;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.nio.file.StandardOpenOption.READ;

public class IndexBufferHandler {
    private static final ConcurrentHashMap<String, List<IndexBlock>> INDEX_MAP = new ConcurrentHashMap<>();

    public static void offerIndex(String tableName, List<IndexBlock> indexBlockList) {
        if (INDEX_MAP.containsKey(tableName)) {
            List<IndexBlock> list = INDEX_MAP.get(tableName);
            list.addAll(indexBlockList);
        } else {
            INDEX_MAP.put(tableName, new CopyOnWriteArrayList<>(indexBlockList));
        }
    }

    public static List<IndexBlock> getIndexBlocks(String tableName) {
        List<IndexBlock> indexBlockList = INDEX_MAP.get(tableName);
        if (indexBlockList == null) {
            return new ArrayList<>();
        }
        return indexBlockList;
    }

    public static void shutdown() {
        INDEX_MAP.clear();
    }

    public static void initIndexBuffer(File ipFile, BlockingQueue<byte[]> writeRequestQueue) throws IOException {
        FileChannel fileChannel = FileChannel.open(ipFile.toPath(), READ);
        if (fileChannel.size() == 0) {
            System.out.println(">>> no need load index data");
            return;
        }
        System.out.println(">>> load exist index data begin");
        System.out.println(">>> exist index file size: " + fileChannel.size());
        MappedByteBuffer dataByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        long start = System.currentTimeMillis();
        while (dataByteBuffer.hasRemaining()) {
            //读取索引长度
            int indexDataLength = dataByteBuffer.getInt();

            byte[] indexDataByte = new byte[indexDataLength];
            dataByteBuffer.get(indexDataByte);

            writeRequestQueue.offer(indexDataByte);
        }
        fileChannel.close();
        long end = System.currentTimeMillis();
        System.out.println("----- load exist index time: " + (end - start));
        System.out.println(">>> exist index data size: " + INDEX_MAP.size());
        System.out.println(">>> load exist index data complete");
    }
}
