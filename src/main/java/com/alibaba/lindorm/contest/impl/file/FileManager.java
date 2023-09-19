package com.alibaba.lindorm.contest.impl.file;

import com.alibaba.lindorm.contest.impl.common.CommonSetting;
import com.alibaba.lindorm.contest.impl.schema.SchemaMeta;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.READ;

public class FileManager {
    private Map<String, Map<Vin, Map<String, FileChannel>>> writeFileMap = new ConcurrentHashMap<>();
    private Map<String, Map<Vin, Map<String, FileChannel>>> readFileMap = new ConcurrentHashMap<>();
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
                Map<Vin, Map<String, FileChannel>> readFileChannelMap = new ConcurrentHashMap<>();
                Map<Vin, Map<String, FileChannel>> writeFileChannelMap = new ConcurrentHashMap<>();
                File[] dataFiles = dir.listFiles();
                if (dataFiles != null) {
                    for (File dataFile : dataFiles) {
                        String name = dataFile.getName();
                        if (name.equals(CommonSetting.LATEST_INDEX_FILE_NAME)) {
                            FileChannel latestIndexFileChannel = FileChannel.open(dataFile.toPath(), READ);
                            readLatestIndexFileMap.put(tableName, latestIndexFileChannel);
                            continue;
                        }
                        String[] split = name.split(CommonSetting.FILE_SPLIT);
                        String vinName = split[0];
                        String columnName = split[1];
                        Vin vin = new Vin(vinName.getBytes());

                        FileChannel readFileChannel = FileChannel.open(dataFile.toPath(), READ);
                        if (readFileChannelMap.containsKey(vin)) {
                            Map<String, FileChannel> map = readFileChannelMap.get(vin);
                            map.put(columnName, readFileChannel);
                        } else {
                            Map<String, FileChannel> map = new HashMap<>();
                            map.put(columnName, readFileChannel);
                            readFileChannelMap.put(vin, map);
                        }

                        FileChannel writeFileChannel = FileChannel.open(dataFile.toPath(), APPEND);
                        if (writeFileChannelMap.containsKey(vin)) {
                            Map<String, FileChannel> map = writeFileChannelMap.get(vin);
                            map.put(columnName, writeFileChannel);
                        } else {
                            Map<String, FileChannel> map = new HashMap<>();
                            map.put(columnName, writeFileChannel);
                            writeFileChannelMap.put(vin, map);
                        }
                    }
                    writeFileMap.put(tableName, writeFileChannelMap);
                    readFileMap.put(tableName, readFileChannelMap);
                }
            }
        }
    }

    public FileChannel getWriteFilChannel(String tableName, Vin vin, String columnName) throws IOException {
        Map<Vin, Map<String, FileChannel>> channelMap = writeFileMap.get(tableName);
        if (channelMap != null) {
            Map<String, FileChannel> channel = channelMap.get(vin);
            if (channel != null) {
                FileChannel fileChannel = channel.get(columnName);
                if (fileChannel != null) {
                    return fileChannel;
                }
            }
        }

        String absolutePath = dataPath.getAbsolutePath();
        String folder = absolutePath + File.separator + tableName;
        File tablePath = new File(folder);
        if (!tablePath.exists()) {
            tablePath.mkdir();
        }
        String fileName = new String(vin.getVin()) + CommonSetting.FILE_SPLIT + columnName;
        String s = folder + File.separator + fileName;
        File f = new File(s);
        if (!f.exists()) {
            f.createNewFile();
        }

        FileChannel writeFileChannel = FileChannel.open(f.toPath(), APPEND);
        Map<Vin, Map<String, FileChannel>> wtireMap = writeFileMap.get(tableName);
        if (wtireMap != null) {
            Map<String, FileChannel> map = wtireMap.get(vin);
            if (map == null) {
                Map<String, FileChannel> m = new HashMap<>();
                m.put(columnName, writeFileChannel);
                wtireMap.put(vin, m);
            }
        } else {
            wtireMap = new ConcurrentHashMap<>();
            Map<String, FileChannel> m = new HashMap<>();
            m.put(columnName, writeFileChannel);
            wtireMap.put(vin, m);
            writeFileMap.put(tableName, wtireMap);
        }

        return writeFileChannel;
    }

    public FileChannel getReadFileChannel(String tableName, Vin vin, String columnName) throws IOException {
        Map<Vin, Map<String, FileChannel>> vinFileChannelMap = readFileMap.get(tableName);
        if (vinFileChannelMap != null) {
            Map<String, FileChannel> fileChannel = vinFileChannelMap.get(vin);
            if (fileChannel != null) {
                FileChannel f = fileChannel.get(columnName);
                if (f != null) {
                    return f;
                }
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
        Map<String, FileChannel> map = new HashMap<>();
        map.put(columnName, channel);
        vinFileChannelMap = new ConcurrentHashMap<>();
        vinFileChannelMap.put(vin, map);
        readFileMap.put(tableName, vinFileChannelMap);
        return channel;
    }

    public void shutdown() {
        try {
            for (Map.Entry<String, Map<Vin, Map<String, FileChannel>>> e : writeFileMap.entrySet()) {
                for (Map.Entry<Vin, Map<String, FileChannel>> file : e.getValue().entrySet()) {
                    for (Map.Entry<String, FileChannel> entry : file.getValue().entrySet()) {
                        entry.getValue().force(false);
                        entry.getValue().close();
                    }
                }
            }
            for (Map.Entry<String, Map<Vin, Map<String, FileChannel>>> e : readFileMap.entrySet()) {
                for (Map.Entry<Vin, Map<String, FileChannel>> file : e.getValue().entrySet()) {
                    for (Map.Entry<String, FileChannel> entry : file.getValue().entrySet()) {
                        entry.getValue().close();
                    }
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
        } catch (Exception e) {
            System.out.println(">>> " + Thread.currentThread().getName() + " FileManager happen exception: " + e.getMessage());
            for (StackTraceElement stackTraceElement : e.getStackTrace()) {
                System.out.println(">>> " + Thread.currentThread().getName() + " FileManager happen exception: " + stackTraceElement.toString());
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
