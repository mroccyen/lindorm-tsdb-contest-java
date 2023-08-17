package com.alibaba.lindorm.contest.impl.file;

import com.alibaba.lindorm.contest.impl.schema.SchemaMeta;
import com.alibaba.lindorm.contest.impl.common.CommonSetting;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.READ;

public class FileManager {
    private Map<String, Map<Vin, FileChannel>> writeFileMap = new ConcurrentHashMap<>();
    private Map<String, Map<Vin, Lock>> writeLockMap = new ConcurrentHashMap<>();
    private Map<String, Map<Vin, FileChannel>> readFileMap = new ConcurrentHashMap<>();
    private final File dataPath;
    private final Map<String, SchemaMeta> tableSchemaMetaMap = new ConcurrentHashMap<>();
    private Map<String, FileChannel> writeLatestIndexFileMap = new ConcurrentHashMap<>();
    private Map<String, FileChannel> readLatestIndexFileMap = new ConcurrentHashMap<>();

    public FileManager(File dataPath) {
        this.dataPath = dataPath;
    }

    public void loadExistFile() throws IOException {
        File[] files = dataPath.listFiles();
        if (files != null) {
            for (File dir : files) {
                String tableName = dir.getName();
                Map<Vin, FileChannel> readFileChannelMap = new ConcurrentHashMap<>();
                Map<Vin, FileChannel> writeFileChannelMap = new ConcurrentHashMap<>();
                Map<Vin, Lock> writeFileLock = new ConcurrentHashMap<>();
                File[] dataFiles = dir.listFiles();
                if (dataFiles != null) {
                    for (File dataFile : dataFiles) {
                        String name = dataFile.getName();
                        if (name.equals(CommonSetting.LATEST_INDEX_FILE_NAME)) {
                            FileChannel latestIndexFileChannel = FileChannel.open(dataFile.toPath(), READ);
                            readLatestIndexFileMap.put(tableName, latestIndexFileChannel);
                            continue;
                        }
                        Vin vin = new Vin(name.getBytes());

                        FileChannel readFileChannel = FileChannel.open(dataFile.toPath(), READ);
                        readFileChannelMap.put(vin, readFileChannel);

                        FileChannel writeFileChannel = FileChannel.open(dataFile.toPath(), APPEND);
                        writeFileChannelMap.put(vin, writeFileChannel);

                        writeFileLock.put(vin, new ReentrantLock());
                    }
                    writeFileMap.put(tableName, writeFileChannelMap);
                    writeLockMap.put(tableName, writeFileLock);
                    readFileMap.put(tableName, readFileChannelMap);
                }
            }
        }
    }

    public FileChannel getWriteFilChannel(String tableName, Vin vin) throws IOException {
        Map<Vin, FileChannel> channelMap = writeFileMap.get(tableName);
        if (channelMap != null) {
            FileChannel channel = channelMap.get(vin);
            if (channel != null) {
                return channel;
            }
        }
        //加锁
        Lock writeLock = getWriteLock(tableName, vin);
        writeLock.lock();

        //拿到锁后先查询一次，可能会出现之前有线程创建了
        Map<Vin, FileChannel> fileChannelMap = writeFileMap.get(tableName);
        if (fileChannelMap != null) {
            FileChannel channel = fileChannelMap.get(vin);
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
        String fileName = new String(vin.getVin());
        String s = folder + File.separator + fileName;
        File f = new File(s);
        if (!f.exists()) {
            f.createNewFile();
        }

        FileChannel writeFileChannel = FileChannel.open(f.toPath(), APPEND);
        //插入最新值的偏移量
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(8);
        writeFileChannel.write(buffer);
        Map<Vin, FileChannel> wtireMap = writeFileMap.get(tableName);
        if (wtireMap != null) {
            wtireMap.put(vin, writeFileChannel);
        } else {
            wtireMap = new ConcurrentHashMap<>();
            wtireMap.put(vin, writeFileChannel);
            writeFileMap.put(tableName, wtireMap);
        }

        //释放锁
        writeLock.unlock();

        return writeFileChannel;
    }

    public FileChannel getReadFileChannel(String tableName, Vin vin) throws IOException {
        Map<Vin, FileChannel> vinFileChannelMap = readFileMap.get(tableName);
        if (vinFileChannelMap != null) {
            FileChannel fileChannel = vinFileChannelMap.get(vin);
            if (fileChannel != null) {
                return fileChannel;
            }
        }

        String absolutePath = dataPath.getAbsolutePath();
        String folder = absolutePath + File.separator + tableName;
        File tablePath = new File(folder);
        if (!tablePath.exists()) {
            return null;
        }
        String fileName = new String(vin.getVin());
        String s = folder + File.separator + fileName;
        File f = new File(s);
        if (!f.exists()) {
            return null;
        }
        FileChannel channel = FileChannel.open(f.toPath(), READ);
        vinFileChannelMap = new ConcurrentHashMap<>();
        vinFileChannelMap.put(vin, channel);
        readFileMap.put(tableName, vinFileChannelMap);
        return channel;
    }

    public Lock getWriteLock(String tableName, Vin vin) {
        Map<Vin, Lock> lockMap = writeLockMap.get(tableName);
        Lock writeLock = lockMap.get(vin);
        if (writeLock != null) {
            return writeLock;
        }
        return lockMap.computeIfAbsent(vin, key -> new ReentrantLock());
    }

    public void shutdown() {
        try {
            for (Map.Entry<String, Map<Vin, FileChannel>> e : writeFileMap.entrySet()) {
                for (Map.Entry<Vin, FileChannel> file : e.getValue().entrySet()) {
                    file.getValue().force(false);
                    file.getValue().close();
                }
            }
            for (Map.Entry<String, Map<Vin, FileChannel>> e : readFileMap.entrySet()) {
                for (Map.Entry<Vin, FileChannel> file : e.getValue().entrySet()) {
                    file.getValue().close();
                }
            }
            for (Map.Entry<String, FileChannel> file : writeLatestIndexFileMap.entrySet()) {
                file.getValue().close();
            }
            for (Map.Entry<String, FileChannel> file : readLatestIndexFileMap.entrySet()) {
                file.getValue().close();
            }
            writeFileMap = null;
            writeLockMap = null;
            readFileMap = null;
            writeLatestIndexFileMap = null;
            readLatestIndexFileMap = null;
        } catch (Exception e) {
            System.out.println(">>> " + Thread.currentThread().getName() + " FileManager happen exception: " + e.getMessage());
            for (StackTraceElement stackTraceElement : e.getStackTrace()) {
                System.out.println(">>> " + Thread.currentThread().getName() + " FileManager happen exception: " + stackTraceElement.toString());
            }
            System.exit(-1);
        }
    }

    public Map<String, Map<Vin, FileChannel>> getReadFileMap() {
        return readFileMap;
    }

    public void initTableWriteLockMap(String tableName) {
        writeLockMap.put(tableName, new ConcurrentHashMap<>());
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

    public FileChannel getWriteLatestIndexFile(String tableName) throws IOException {
        if (writeLatestIndexFileMap.containsKey(tableName)) {
            return writeLatestIndexFileMap.get(tableName);
        }
        String absolutePath = dataPath.getAbsolutePath();
        String folder = absolutePath + File.separator + tableName;
        File tablePath = new File(folder);
        if (!tablePath.exists()) {
            return null;
        }
        String s = folder + File.separator + CommonSetting.LATEST_INDEX_FILE_NAME;
        File f = new File(s);
        if (!f.exists()) {
            f.createNewFile();
        } else {
            f.delete();
            f.createNewFile();
        }
        FileChannel channel = FileChannel.open(f.toPath(), APPEND);
        writeLatestIndexFileMap.put(tableName, channel);
        return channel;
    }

    public Map<String, FileChannel> getReadLatestIndexFileMap() {
        return readLatestIndexFileMap;
    }
}
