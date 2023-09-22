package com.alibaba.lindorm.contest.impl.file;

import com.alibaba.lindorm.contest.impl.common.CommonSetting;
import com.alibaba.lindorm.contest.impl.schema.SchemaMeta;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.READ;

public class FileManager {
    private Map<String, Map<String, FileChannel>> writeFileMap = new ConcurrentHashMap<>();
    private Map<String, Map<String, FileChannel>> readFileMap = new ConcurrentHashMap<>();
    private final File dataPath;
    private final Map<String, SchemaMeta> tableSchemaMetaMap = new ConcurrentHashMap<>();
    private Map<String, FileChannel> writeLatestIndexFileMap = new ConcurrentHashMap<>();
    private Map<String, FileChannel> readLatestIndexFileMap = new ConcurrentHashMap<>();
    private boolean isWriting = false;

    public FileManager(File dataPath) {
        this.dataPath = dataPath;
    }

    public boolean isWriting() {
        return isWriting;
    }

    public void startWrite() {
        isWriting = true;
    }

    public void endWrite() {
        isWriting = false;
    }

    public void loadExistFile() throws IOException {
        File[] files = dataPath.listFiles();
        if (files != null) {
            for (File dir : files) {
                String tableName = dir.getName();
                Map<String, FileChannel> readFileChannelMap = new ConcurrentHashMap<>();
                Map<String, FileChannel> writeFileChannelMap = new ConcurrentHashMap<>();
                File[] dataFiles = dir.listFiles();
                if (dataFiles != null) {
                    for (File dataFile : dataFiles) {
                        String name = dataFile.getName();
                        if (name.equals(CommonSetting.LATEST_INDEX_FILE_NAME)) {
                            FileChannel latestIndexFileChannel = FileChannel.open(dataFile.toPath(), READ);
                            readLatestIndexFileMap.put(tableName, latestIndexFileChannel);
                            continue;
                        }

                        FileChannel readFileChannel = FileChannel.open(dataFile.toPath(), READ);
                        readFileChannelMap.put(name, readFileChannel);

                        FileChannel writeFileChannel = FileChannel.open(dataFile.toPath(), APPEND);
                        writeFileChannelMap.put(name, writeFileChannel);
                    }
                    writeFileMap.put(tableName, writeFileChannelMap);
                    readFileMap.put(tableName, readFileChannelMap);
                }
            }
        }
    }

    public FileChannel getWriteFilChannel(String tableName, String name) throws IOException {
        Map<String, FileChannel> channelMap = writeFileMap.get(tableName);
        if (channelMap != null) {
            FileChannel channel = channelMap.get(name);
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
        String s = folder + File.separator + name;
        File f = new File(s);
        if (!f.exists()) {
            f.createNewFile();
        }

        FileChannel writeFileChannel = FileChannel.open(f.toPath(), APPEND);
        Map<String, FileChannel> wtireMap = writeFileMap.get(tableName);
        if (wtireMap != null) {
            wtireMap.put(name, writeFileChannel);
        } else {
            wtireMap = new ConcurrentHashMap<>();
            wtireMap.put(name, writeFileChannel);
            writeFileMap.put(tableName, wtireMap);
        }
        getReadFileChannel(tableName, name);

        return writeFileChannel;
    }

    public FileChannel getReadFileChannel(String tableName, String name) throws IOException {
        Map<String, FileChannel> vinFileChannelMap = readFileMap.get(tableName);
        if (vinFileChannelMap != null) {
            FileChannel fileChannel = vinFileChannelMap.get(name);
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
        String s = folder + File.separator + name;
        File f = new File(s);
        if (!f.exists()) {
            return null;
        }
        FileChannel channel = FileChannel.open(f.toPath(), READ);
        vinFileChannelMap = new ConcurrentHashMap<>();
        vinFileChannelMap.put(name, channel);
        readFileMap.put(tableName, vinFileChannelMap);
        return channel;
    }

    public void shutdown() {
        try {
            for (Map.Entry<String, Map<String, FileChannel>> e : writeFileMap.entrySet()) {
                for (Map.Entry<String, FileChannel> file : e.getValue().entrySet()) {
                    file.getValue().force(false);
                    file.getValue().close();
                }
            }
            for (Map.Entry<String, Map<String, FileChannel>> e : readFileMap.entrySet()) {
                for (Map.Entry<String, FileChannel> file : e.getValue().entrySet()) {
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
            readFileMap = null;
            writeLatestIndexFileMap = null;
            readLatestIndexFileMap = null;
            endWrite();
        } catch (Exception e) {
            System.out.println(">>> " + Thread.currentThread().getName() + " FileManager happen exception: " + e.getMessage());
            for (StackTraceElement stackTraceElement : e.getStackTrace()) {
                System.out.println(">>> " + Thread.currentThread().getName() + " FileManager happen exception: " + stackTraceElement.toString());
            }
            System.exit(-1);
        }
    }

    public void flush() {
        try {
            for (Map.Entry<String, Map<String, FileChannel>> e : writeFileMap.entrySet()) {
                for (Map.Entry<String, FileChannel> file : e.getValue().entrySet()) {
                    file.getValue().force(false);
                }
            }
        } catch (Exception e) {
            System.out.println(">>> " + Thread.currentThread().getName() + " FileManager flush happen exception: " + e.getMessage());
            for (StackTraceElement stackTraceElement : e.getStackTrace()) {
                System.out.println(">>> " + Thread.currentThread().getName() + " FileManager flush happen exception: " + stackTraceElement.toString());
            }
            System.exit(-1);
        }
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
