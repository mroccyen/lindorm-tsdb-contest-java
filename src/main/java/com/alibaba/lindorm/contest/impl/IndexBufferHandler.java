package com.alibaba.lindorm.contest.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardOpenOption.READ;

public class IndexBufferHandler {
    private static final ConcurrentHashMap<String, List<IndexBlock>> INDEX_MAP = new ConcurrentHashMap<>();

    public static synchronized void offerIndex(String tableName, List<IndexBlock> indexBlockList) {
        if (INDEX_MAP.containsKey(tableName)) {
            List<IndexBlock> list = INDEX_MAP.get(tableName);
            list.addAll(indexBlockList);
        } else {
            INDEX_MAP.put(tableName, new ArrayList<>(indexBlockList));
        }
    }

    public static synchronized void shutdown() {
        INDEX_MAP.clear();
    }

    public static void initIndexBuffer(File ipFile) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024);
        FileChannel fileChannel = FileChannel.open(ipFile.toPath(), READ);
        if (fileChannel.size() == 0) {
            return;
        }
        int read = fileChannel.read(byteBuffer);
        while (read != -1) {
            byteBuffer.flip();
            int dataPosition = byteBuffer.getInt();
            short tableNameLength = byteBuffer.getShort();
            byte[] tableName = new byte[tableNameLength];
            for (short i = 0; i < tableNameLength; i++) {
                tableName[i] = byteBuffer.get();
            }
            int i = 0;
            byteBuffer.clear();
            read = fileChannel.read(byteBuffer);
        }
    }
}
