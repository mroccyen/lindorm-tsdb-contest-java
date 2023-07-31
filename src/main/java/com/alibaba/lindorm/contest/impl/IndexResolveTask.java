package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.Vin;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class IndexResolveTask extends Thread {
    private final BlockingQueue<IndexLoadCompleteNotice> writeRequestQueue = new ArrayBlockingQueue<>(50);

    private boolean stop = false;

    private IndexLoadCompleteWrapper indexLoadCompleteWrapper;

    private long size;

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
        while (!stop) {
            try {
                IndexLoadCompleteNotice notice = writeRequestQueue.poll(5, TimeUnit.MILLISECONDS);
                if (notice != null) {
                    if (notice.isComplete()) {
                        indexLoadCompleteWrapper.getLock().lock();
                        indexLoadCompleteWrapper.getCondition().signal();
                        indexLoadCompleteWrapper.getLock().unlock();
                        System.out.println(">>> IndexResolveTask load index data size: " + size);
                    } else {
                        size++;
                        byte[] poll = notice.getIndexDataByte();
                        if (poll != null && poll.length > 0) {
                            resolve(poll);
                        }
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

        byte[] timeStampByte = new byte[8];
        for (int j = i; j < i + 8; j++) {
            timeStampByte[j - i] = indexByteList[j];
        }
        long timeStamp = ByteArrayUtil.byteArray2Long_Big_Endian(timeStampByte);
        indexBlock.setTimestamp(timeStamp);
        i = i + 8;

        byte index = indexByteList[i];
        i = i + 1;
        indexBlock.setIndex(index);

        byte[] dataSizeByte = new byte[4];
        for (int j = i; j < i + 4; j++) {
            dataSizeByte[j - i] = indexByteList[j];
        }
        int dataSize = ByteArrayUtil.byteArray2Int_Big_Endian(dataSizeByte);
        indexBlock.setDataSize(dataSize);
        i = i + 4;

        byte tableNameLength = indexByteList[i];
        indexBlock.setTableNameLength(tableNameLength);
        i = i + 1;

        byte[] tableName = new byte[tableNameLength];
        for (int j = i; j < i + tableNameLength; j++) {
            tableName[j - i] = indexByteList[j];
        }
        indexBlock.setTableName(tableName);
        i = i + tableNameLength;

        int rowKeyLength = Vin.VIN_LENGTH;
        byte[] rowKey = new byte[rowKeyLength];
        for (int j = i; j < i + rowKeyLength; j++) {
            rowKey[j - i] = indexByteList[j];
        }
        indexBlock.setRowKey(rowKey);

        IndexBufferHandler.offerIndex(new String(tableName), indexBlock);
    }
}