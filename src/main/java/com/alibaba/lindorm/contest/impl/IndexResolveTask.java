package com.alibaba.lindorm.contest.impl;

import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class IndexResolveTask extends Thread {
    private final BlockingQueue<byte[]> writeRequestQueue = new ArrayBlockingQueue<>(50);

    private boolean stop = false;

    public void shutdown() {
        stop = true;
    }

    public BlockingQueue<byte[]> getWriteRequestQueue() {
        return writeRequestQueue;
    }

    @Override
    public void run() {
        try {
            while (!stop) {
                byte[] poll = writeRequestQueue.poll(5, TimeUnit.MILLISECONDS);
                if (poll != null && poll.length > 0) {
                    resolve(poll);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        }
    }

    public void resolve(byte[] indexByteList) {
        int i = 0;
        IndexBlock indexBlock = new IndexBlock();

        byte[] offsetByte = new byte[8];
        for (int j = i; j < i + 8; j++) {
            offsetByte[j - i] = indexByteList[j];
        }
        long offset = ByteArrayUtil.byteArray2Long_Big_Endian(offsetByte);
        indexBlock.setOffset(offset);
        i = i + 8;

        byte[] dataSizeByte = new byte[4];
        for (int j = i; j < i + 4; j++) {
            dataSizeByte[j - i] = indexByteList[j];
        }
        int dataSize = ByteArrayUtil.byteArray2Int_Big_Endian(dataSizeByte);
        indexBlock.setDataSize(dataSize);
        i = i + 4;

        byte[] tableNameLengthByte = new byte[4];
        for (int j = i; j < i + 4; j++) {
            tableNameLengthByte[j - i] = indexByteList[j];
        }
        int tableNameLength = ByteArrayUtil.byteArray2Int_Big_Endian(tableNameLengthByte);
        indexBlock.setTableNameLength(tableNameLength);
        i = i + 4;

        byte[] tableName = new byte[tableNameLength];
        for (int j = i; j < i + tableNameLength; j++) {
            tableName[j - i] = indexByteList[j];
        }
        indexBlock.setTableName(tableName);
        i = i + tableNameLength;

        byte[] rowKeyLengthByte = new byte[4];
        for (int j = i; j < i + 4; j++) {
            rowKeyLengthByte[j - i] = indexByteList[j];
        }
        int rowKeyLength = ByteArrayUtil.byteArray2Int_Big_Endian(rowKeyLengthByte);
        indexBlock.setRowKeyLength(rowKeyLength);
        i = i + 4;

        byte[] rowKey = new byte[rowKeyLength];
        for (int j = i; j < i + rowKeyLength; j++) {
            rowKey[j - i] = indexByteList[j];
        }
        indexBlock.setRowKey(rowKey);

        IndexBufferHandler.offerIndex(new String(tableName), Collections.singletonList(indexBlock));
    }
}