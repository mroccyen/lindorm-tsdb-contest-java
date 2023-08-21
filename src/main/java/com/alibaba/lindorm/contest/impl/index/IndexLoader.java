package com.alibaba.lindorm.contest.impl.index;

import com.alibaba.lindorm.contest.impl.file.FileManager;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IndexLoader {
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Vin, Index>> LATEST_INDEX_CACHE_MAP = new ConcurrentHashMap<>();

    public static boolean offerLatestIndex(String tableName, Vin vin, Index index) {
        boolean flag = false;
        if (LATEST_INDEX_CACHE_MAP.containsKey(tableName)) {
            ConcurrentHashMap<Vin, Index> map = LATEST_INDEX_CACHE_MAP.get(tableName);
            Index i = map.get(vin);
            if (i == null || i.getLatestTimestamp() <= index.getLatestTimestamp()) {
                map.put(vin, index);
                flag = true;
            }
        } else {
            ConcurrentHashMap<Vin, Index> map = new ConcurrentHashMap<>();
            map.put(vin, index);
            LATEST_INDEX_CACHE_MAP.put(tableName, map);
            flag = true;
        }
        return flag;
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
        ByteBuffer buffer = ByteBuffer.allocate(8);
        System.out.println(">>> initIndexBuffer load exist index data begin");
        long start = System.currentTimeMillis();
        for (Map.Entry<String, Map<Vin, FileChannel>> mapEntry : fileManager.getReadFileMap().entrySet()) {
            String tableName = mapEntry.getKey();
            for (Map.Entry<Vin, FileChannel> channelEntry : mapEntry.getValue().entrySet()) {
                Vin vin = channelEntry.getKey();
                FileChannel fileChannel = channelEntry.getValue();
                if (fileChannel == null || fileChannel.size() == 0) {
                    continue;
                }
                fileChannel.read(buffer);
                buffer.flip();
                long offset = buffer.getLong();

                IndexLoadCompleteNotice notice = new IndexLoadCompleteNotice();
                notice.setComplete(false);
                notice.setTableName(tableName);
                notice.setOffset(offset);
                notice.setVin(vin.getVin());
                try {
                    indexLoaderTask.getWriteRequestQueue().put(notice);
                } catch (InterruptedException e) {
                    System.out.println(e.getMessage());
                    System.exit(-1);
                }

                IndexLoadCompleteWrapper wrapper = new IndexLoadCompleteWrapper();
                wrapper.getLock().lock();
                //通知等待完成
                indexLoaderTask.waitComplete(wrapper);
                //结束进行通知
                notice = new IndexLoadCompleteNotice();
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

                buffer.clear();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(">>> initIndexBuffer load exist index time: " + (end - start));
        System.out.println(">>> initIndexBuffer load exist index data complete");
    }
}
