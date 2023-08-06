package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IndexLoader {
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Vin, Index>> LATEST_INDEX_CACHE_MAP = new ConcurrentHashMap<>();

    public static void offerLatestIndex(String tableName, Vin vin, Index index) {
        if (LATEST_INDEX_CACHE_MAP.containsKey(tableName)) {
            ConcurrentHashMap<Vin, Index> map = LATEST_INDEX_CACHE_MAP.get(tableName);
            Index i = map.get(vin);
            if (i == null || i.getLatestTimestamp() <= index.getLatestTimestamp()) {
                map.put(vin, index);
            }
        } else {
            ConcurrentHashMap<Vin, Index> map = new ConcurrentHashMap<>();
            map.put(vin, index);
            LATEST_INDEX_CACHE_MAP.put(tableName, map);
        }
    }

    public static Index getLatestIndex(String tableName, Vin vin) {
        ConcurrentHashMap<Vin, Index> map = LATEST_INDEX_CACHE_MAP.get(tableName);
        if (map == null) {
            return null;
        }
        return map.get(vin);
    }

    public static void shutdown() {
        LATEST_INDEX_CACHE_MAP.clear();
    }

    public static void loadLatestIndex(FileManager fileManager, IndexLoaderTask indexLoaderTask) throws IOException {
        System.out.println(">>> initIndexBuffer load exist index data begin");
        long start = System.currentTimeMillis();
        for (Map.Entry<String, Map<Vin, FileChannel>> mapEntry : fileManager.getReadFileMap().entrySet()) {
            String tableName = mapEntry.getKey();
            SchemaMeta schemaMeta = fileManager.getSchemaMeta(tableName);
            long size = 0;
            for (Map.Entry<Vin, FileChannel> fileChannelEntry : mapEntry.getValue().entrySet()) {
                FileChannel fileChannel = fileChannelEntry.getValue();
                if (fileChannel == null || fileChannel.size() == 0) {
                    continue;
                }
                MappedByteBuffer dataByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
                long position = 0;
                while (dataByteBuffer.hasRemaining()) {
                    long p = 0;
                    byte[] vinByte = new byte[Vin.VIN_LENGTH];
                    for (int i = 0; i < Vin.VIN_LENGTH; i++) {
                        vinByte[i] = dataByteBuffer.get();
                    }
                    p = p + Vin.VIN_LENGTH;
                    long timestamp = dataByteBuffer.getLong();
                    p = p + 8;
                    for (int cI = 0; cI < schemaMeta.getColumnsNum(); ++cI) {
                        ColumnValue.ColumnType cType = schemaMeta.getColumnsType().get(cI);
                        switch (cType) {
                            case COLUMN_TYPE_INTEGER:
                                dataByteBuffer.getInt();
                                p = p + 4;
                                break;
                            case COLUMN_TYPE_DOUBLE_FLOAT:
                                dataByteBuffer.getDouble();
                                p = p + 8;
                                break;
                            case COLUMN_TYPE_STRING:
                                int length = dataByteBuffer.getInt();
                                p = p + 4;
                                for (int i = 0; i < length; i++) {
                                    dataByteBuffer.get();
                                }
                                p = p + length;
                                break;
                            default:
                                throw new IllegalStateException("Undefined column type, this is not expected");
                        }
                    }
                    IndexLoadCompleteNotice notice = new IndexLoadCompleteNotice();
                    notice.setComplete(false);
                    notice.setTableName(tableName);
                    notice.setOffset(position);
                    notice.setTimestamp(timestamp);
                    notice.setVin(vinByte);
                    try {
                        indexLoaderTask.getWriteRequestQueue().put(notice);
                    } catch (InterruptedException e) {
                        System.out.println(e.getMessage());
                        System.exit(-1);
                    }
                    position = position + p;
                }
                IndexLoadCompleteWrapper wrapper = new IndexLoadCompleteWrapper();
                wrapper.getLock().lock();
                //通知等待完成
                indexLoaderTask.waitComplete(wrapper);
                //结束进行通知
                IndexLoadCompleteNotice notice = new IndexLoadCompleteNotice();
                notice.setComplete(true);
                indexLoaderTask.getWriteRequestQueue().offer(notice);
                try {
                    //等待索引数据加载完成
                    wrapper.getCondition().await();
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    System.exit(-1);
                }
                wrapper.getLock().unlock();
                size++;
                //System.out.println(">>> initIndexBuffer complete load exist index data: " + new String(fileChannelEntry.getKey().getVin()));
            }
            System.out.println(">>> table " + tableName + " complete load exist index size: " + size);
        }
        long end = System.currentTimeMillis();
        System.out.println(">>> initIndexBuffer load exist index time: " + (end - start));
        System.out.println(">>> initIndexBuffer load exist index data complete");
    }
}
