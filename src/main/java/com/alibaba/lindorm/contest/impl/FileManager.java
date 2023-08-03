package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.Vin;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.READ;

public class FileManager {
    private final Map<String, Map<Integer, FileChannel>> writeFileMap = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, Lock>> writeLockMap = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, FileChannel>> readFileMap = new ConcurrentHashMap<>();
    private final File dataPath;
    private final Map<String, SchemaMeta> tableSchemaMetaMap = new ConcurrentHashMap<>();

    public FileManager(File dataPath) {
        this.dataPath = dataPath;
    }

    public void loadExistFile() throws IOException {
        File[] files = dataPath.listFiles();
        if (files != null) {
            for (File dir : files) {
                String tableName = dir.getName();
                Map<Integer, FileChannel> readFileChannelMap = new ConcurrentHashMap<>();
                Map<Integer, FileChannel> writeFileChannelMap = new ConcurrentHashMap<>();
                Map<Integer, Lock> writeFileLock = new ConcurrentHashMap<>();
                File[] dataFiles = dir.listFiles();
                if (dataFiles != null) {
                    for (File dataFile : dataFiles) {
                        String name = dataFile.getName();
                        int index = Integer.parseInt(name);

                        FileChannel readFileChannel = FileChannel.open(dataFile.toPath(), READ);
                        readFileChannelMap.put(index, readFileChannel);

                        FileChannel writeFileChannel = FileChannel.open(dataFile.toPath(), APPEND);
                        writeFileChannelMap.put(index, writeFileChannel);

                        writeFileLock.put(index, new ReentrantLock());
                    }
                    readFileMap.put(tableName, readFileChannelMap);
                    writeFileMap.put(tableName, writeFileChannelMap);
                    writeLockMap.put(tableName, writeFileLock);
                }
            }
        }
    }

    public FileChannel getWriteFilChannel(String tableName, Vin vin) throws IOException {
        int folderIndex = vin.hashCode() % CommonSetting.NUM_FOLDERS;
        Map<Integer, FileChannel> channelMap = writeFileMap.get(tableName);
        if (channelMap != null) {
            FileChannel channel = channelMap.get(folderIndex);
            if (channel != null) {
                return channel;
            }
        }
//        //加锁
//        Lock writeLock = getWriteLock(tableName, vin);
//        writeLock.lock();
//
//        //拿到锁后先查询一次，可能会出现之前有线程创建了
//        Map<Integer, FileChannel> fileChannelMap = writeFileMap.get(tableName);
//        if (fileChannelMap != null) {
//            FileChannel channel = fileChannelMap.get(folderIndex);
//            if (channel != null) {
//                return channel;
//            }
//        }

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

        FileChannel readFileChannel = FileChannel.open(f.toPath(), READ);
        Map<Integer, FileChannel> readMap = readFileMap.get(tableName);
        if (readMap != null) {
            readMap.put(folderIndex, readFileChannel);
        } else {
            readMap = new ConcurrentHashMap<>();
            readMap.put(folderIndex, readFileChannel);
            readFileMap.put(tableName, readMap);
        }

        FileChannel writeFileChannel = FileChannel.open(f.toPath(), APPEND);
        Map<Integer, FileChannel> wtireMap = writeFileMap.get(tableName);
        if (wtireMap != null) {
            wtireMap.put(folderIndex, writeFileChannel);
        } else {
            wtireMap = new ConcurrentHashMap<>();
            wtireMap.put(folderIndex, writeFileChannel);
            writeFileMap.put(tableName, wtireMap);
        }

        //释放锁
        //writeLock.unlock();

        return writeFileChannel;
    }

    public Lock getWriteLock(String tableName, Vin vin) {
        int folderIndex = vin.hashCode() % CommonSetting.NUM_FOLDERS;
        Map<Integer, Lock> lockMap = writeLockMap.get(tableName);
        if (lockMap != null) {
            Lock lock = lockMap.get(folderIndex);
            if (lock != null) {
                return lock;
            }
        }
        synchronized (writeLockMap) {
            //拿到锁后先查询一次，可能会出现之前有线程创建了
            Map<Integer, Lock> map = writeLockMap.get(tableName);
            if (map != null) {
                synchronized (writeLockMap) {
                    Lock lock = map.get(folderIndex);
                    if (lock != null) {
                        return lock;
                    }
                }
            }
            if (lockMap != null) {
                return lockMap.computeIfAbsent(folderIndex, key -> new ReentrantLock());
            } else {
                lockMap = new ConcurrentHashMap<>();
                Lock lock = lockMap.computeIfAbsent(folderIndex, key -> new ReentrantLock());
                writeLockMap.put(tableName, lockMap);
                return lock;
            }
        }
    }

    public void shutdown() {
        try {
            for (Map.Entry<String, Map<Integer, FileChannel>> e : writeFileMap.entrySet()) {
                for (Map.Entry<Integer, FileChannel> file : e.getValue().entrySet()) {
                    file.getValue().close();
                }
            }
            for (Map.Entry<String, Map<Integer, FileChannel>> e : readFileMap.entrySet()) {
                for (Map.Entry<Integer, FileChannel> file : e.getValue().entrySet()) {
                    file.getValue().close();
                }
            }
        } catch (Exception e) {
            System.exit(-1);
        }
    }

    public Map<String, Map<Integer, FileChannel>> getWriteFileMap() {
        return writeFileMap;
    }

    public Map<String, Map<Integer, FileChannel>> getReadFileMap() {
        return readFileMap;
    }

    public void addSchemaMeta(String tableName, SchemaMeta schemaMeta) {
        tableSchemaMetaMap.put(tableName, schemaMeta);
    }

    public SchemaMeta getSchemaMeta(String tableName) {
        return tableSchemaMetaMap.get(tableName);
    }

    public Map<String, SchemaMeta> getTableSchemaMetaMap() {
        return tableSchemaMetaMap;
    }
}
