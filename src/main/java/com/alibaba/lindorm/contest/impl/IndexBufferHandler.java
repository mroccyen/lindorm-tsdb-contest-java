package com.alibaba.lindorm.contest.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
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

    public static List<IndexBlock> getIndexBlocks(String tableName) {
        List<IndexBlock> indexBlockList = INDEX_MAP.get(tableName);
        if (indexBlockList == null) {
            return new ArrayList<>();
        }
        return indexBlockList;
    }

    public static synchronized void shutdown() {
        INDEX_MAP.clear();
    }

    public static void initIndexBuffer(File ipFile) throws IOException {
        ByteBuffer sizeByteBuffer = ByteBuffer.allocateDirect(2);
        FileChannel fileChannel = FileChannel.open(ipFile.toPath(), READ);
        if (fileChannel.size() == 0) {
            return;
        }
        int sizeByteBufferRead = fileChannel.read(sizeByteBuffer);
        while (sizeByteBufferRead != -1) {
            IndexBlock indexBlock = new IndexBlock();

            sizeByteBuffer.flip();
            short indexBlockLength = sizeByteBuffer.getShort();
            ByteBuffer dataByteBuffer = ByteBuffer.allocateDirect(indexBlockLength);
            fileChannel.read(dataByteBuffer);
            dataByteBuffer.flip();
            int offset = dataByteBuffer.getInt();
            indexBlock.setOffset(offset);
            short dataSize = dataByteBuffer.getShort();
            indexBlock.setDataSize(dataSize);
            short tableNameLength = dataByteBuffer.getShort();
            indexBlock.setTableNameLength(tableNameLength);
            byte[] tableName = new byte[tableNameLength];
            for (short i = 0; i < tableNameLength; i++) {
                tableName[i] = dataByteBuffer.get();
            }
            indexBlock.setTableName(tableName);
            short rowKeyLength = dataByteBuffer.getShort();
            indexBlock.setRowKeyLength(rowKeyLength);
            byte[] rowKey = new byte[rowKeyLength];
            for (short i = 0; i < rowKeyLength; i++) {
                rowKey[i] = dataByteBuffer.get();
            }
            indexBlock.setRowKey(rowKey);
            offerIndex(new String(tableName), Collections.singletonList(indexBlock));

            dataByteBuffer = null;
            sizeByteBuffer.clear();
            sizeByteBufferRead = fileChannel.read(sizeByteBuffer);
        }
        sizeByteBuffer = null;
        fileChannel.close();
    }
}
