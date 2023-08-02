package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.Vin;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import static java.nio.file.StandardOpenOption.APPEND;

public class FileManager {
    private static final int NUM_FOLDERS = 300;
    private final Map<String, Map<Integer, FileChannel>> fileMap = new HashMap<>();
    private final File dataPath;

    public FileManager(File dataPath) {
        this.dataPath = dataPath;
    }

    public FileChannel getWriteFilChannel(String tableName, Vin vin) throws IOException {
        int folderIndex = vin.hashCode() % NUM_FOLDERS;
        Map<Integer, FileChannel> channelMap = fileMap.get(tableName);
        if (channelMap != null) {
            FileChannel channel = channelMap.get(folderIndex);
            if (channel != null) {
                return channel;
            }
        }
        String absolutePath = dataPath.getAbsolutePath();
        String folder = absolutePath + File.separator + tableName;
        File tablePath = new File(folder);
        if (!tablePath.exists()) {
            tablePath.mkdir();
        }
        String s = folder + File.separator + folderIndex;
        File f = new File(s);
        if (!f.exists()) {
            f.createNewFile();
        }
        synchronized (fileMap) {
            //拿到锁后先查询一次，可能会出现之前有线程创建了
            Map<Integer, FileChannel> fileChannelMap = fileMap.get(tableName);
            if (fileChannelMap != null) {
                synchronized (fileMap) {
                    FileChannel channel = fileChannelMap.get(folderIndex);
                    if (channel != null) {
                        return channel;
                    }
                }
            }
            FileChannel fileChannel = FileChannel.open(f.toPath(), APPEND);
            Map<Integer, FileChannel> map = fileMap.get(tableName);
            if (map != null) {
                map.put(folderIndex, fileChannel);
            } else {
                map = new HashMap<>();
                map.put(folderIndex, fileChannel);
                fileMap.put(tableName, map);
            }
            return fileChannel;
        }
    }

    public void shutdown() {

    }

    public Map<String, Map<Integer, FileChannel>> getFileMap() {
        return fileMap;
    }
}
