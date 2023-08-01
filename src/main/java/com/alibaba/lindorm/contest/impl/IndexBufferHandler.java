package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.impl.bpluse.BTree;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardOpenOption.READ;

public class IndexBufferHandler {
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, BTree<Long>>> INDEX_CACHE_MAP = new ConcurrentHashMap<>();

    public static void offerIndex(String tableName, IndexBlock indexBlock) {
        String rowKey = new String(indexBlock.getRowKey());
        if (INDEX_CACHE_MAP.containsKey(tableName)) {
            ConcurrentHashMap<String, BTree<Long>> map = INDEX_CACHE_MAP.get(tableName);
            if (map.containsKey(rowKey)) {
                BTree<Long> tree = map.get(rowKey);
                tree.insert(indexBlock.getTimestamp(), indexBlock);
            } else {
                BTree<Long> tree = new BTree<>(30);
                tree.insert(indexBlock.getTimestamp(), indexBlock);
                map.put(rowKey, tree);
            }
        } else {
            ConcurrentHashMap<String, BTree<Long>> map = new ConcurrentHashMap<>();
            BTree<Long> tree = new BTree<>(30);
            tree.insert(indexBlock.getTimestamp(), indexBlock);
            map.put(rowKey, tree);
            INDEX_CACHE_MAP.put(tableName, map);
        }
    }

    public static ConcurrentHashMap<String, BTree<Long>> getIndexBlockMap(String tableName) {
        ConcurrentHashMap<String, BTree<Long>> map = INDEX_CACHE_MAP.get(tableName);
        if (map == null) {
            return new ConcurrentHashMap<>();
        }
        return map;
    }

    public static void shutdown() {
        INDEX_CACHE_MAP.clear();
    }

    public static void initIndexBuffer(Map<String, Map<Integer, FileManager.FilePear>> fileMap, IndexResolveTask indexResolveTask) throws IOException {
        for (Map.Entry<String, Map<Integer, FileManager.FilePear>> mapEntry : fileMap.entrySet()) {
            for (Map.Entry<Integer, FileManager.FilePear> ipFile : mapEntry.getValue().entrySet()) {
                FileChannel fileChannel = FileChannel.open(ipFile.getValue().getIpFile().toPath(), READ);
                if (fileChannel.size() == 0) {
                    System.out.println(">>> initIndexBuffer file" + ipFile.getKey() + " no need load index data");
                    continue;
                }
                System.out.println(">>> initIndexBuffer file" + ipFile.getKey() + " load exist index data begin");
                System.out.println(">>> initIndexBuffer file" + ipFile.getKey() + " exist index file size: " + fileChannel.size());
                MappedByteBuffer dataByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
                long start = System.currentTimeMillis();
                long size = 0;
                while (dataByteBuffer.hasRemaining()) {
                    //读取索引长度
                    byte indexDataLength = dataByteBuffer.get();

                    byte[] indexDataByte = new byte[indexDataLength];
                    dataByteBuffer.get(indexDataByte);

                    IndexLoadCompleteNotice notice = new IndexLoadCompleteNotice();
                    notice.setComplete(false);
                    notice.setTableName(mapEntry.getKey());
                    notice.setIndexDataByte(indexDataByte);
                    try {
                        indexResolveTask.getWriteRequestQueue().put(notice);
                    } catch (InterruptedException e) {
                        System.out.println(e.getMessage());
                        System.exit(-1);
                    }
                    size++;
                }
                System.out.println(">>> initIndexBuffer file" + ipFile.getKey() + " offered index data size: " + size);
                //关系通道
                fileChannel.close();

                IndexLoadCompleteWrapper wrapper = new IndexLoadCompleteWrapper();
                wrapper.getLock().lock();
                //通知等待完成
                indexResolveTask.waitComplete(wrapper);
                //结束进行通知
                IndexLoadCompleteNotice notice = new IndexLoadCompleteNotice();
                notice.setComplete(true);
                notice.setIndexDataByte(null);
                indexResolveTask.getWriteRequestQueue().offer(notice);
                try {
                    //等待索引数据加载完成
                    wrapper.getCondition().await();
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    System.exit(-1);
                }
                wrapper.getLock().unlock();
                long end = System.currentTimeMillis();
                System.out.println(">>> initIndexBuffer file" + ipFile.getKey() + " load exist index time: " + (end - start));
                System.out.println(">>> initIndexBuffer file" + ipFile.getKey() + " load exist index data complete");
            }
        }
    }
}
