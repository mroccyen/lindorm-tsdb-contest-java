package com.alibaba.lindorm.contest.impl;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

    public static void initIndexBuffer(File ipFile) throws IOException {
        FileChannel fileChannel = FileChannel.open(ipFile.toPath(), READ);
        if (fileChannel.size() == 0) {
            System.out.println(">>> no need load index data");
            return;
        }
        System.out.println(">>> load exist index data begin");
        System.out.println(">>> exist index data size: " + fileChannel.size());
        MappedByteBuffer dataByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        dataByteBuffer.flip();
        while (dataByteBuffer.hasRemaining()) {
            IndexBlock indexBlock = new IndexBlock();
            //读取索引长度
            dataByteBuffer.getInt();
            long offset = dataByteBuffer.getLong();
            indexBlock.setOffset(offset);
            int dataSize = dataByteBuffer.getInt();
            indexBlock.setDataSize(dataSize);
            int tableNameLength = dataByteBuffer.getInt();
            indexBlock.setTableNameLength(tableNameLength);
            byte[] tableName = new byte[tableNameLength];
            for (int i = 0; i < tableNameLength; i++) {
                tableName[i] = dataByteBuffer.get();
            }
            indexBlock.setTableName(tableName);
            int rowKeyLength = dataByteBuffer.getInt();
            indexBlock.setRowKeyLength(rowKeyLength);
            byte[] rowKey = new byte[rowKeyLength];
            for (int i = 0; i < rowKeyLength; i++) {
                rowKey[i] = dataByteBuffer.get();
            }
            indexBlock.setRowKey(rowKey);
            offerIndex(new String(tableName), Collections.singletonList(indexBlock));
        }
        fileChannel.close();
        System.out.println(">>> load exist index data complete");
    }
}
