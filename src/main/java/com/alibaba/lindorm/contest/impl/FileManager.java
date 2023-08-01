package com.alibaba.lindorm.contest.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FileManager {
    private final Map<Integer, File> dpFileMap = new HashMap<>();
    private final Map<Integer, File> ipFileMap = new HashMap<>();
    private final File dataPath;

    public FileManager(File dataPath) {
        this.dataPath = dataPath;
        File[] files = dataPath.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            String tableName = file.getName();
            File[] listFiles = file.listFiles();
            if (listFiles != null) {
                for (File listFile : listFiles) {
                    String fileName = listFile.getName();
                    String[] split = fileName.split("\\.");
                    if (split[1].equals(CommonSetting.INDEX_EXT)) {
                        String s = split[0];
                        String[] ss = s.split("_");
                        Integer i = Integer.parseInt(ss[1]);
                        ipFileMap.put(i, listFile);
                    }
                    if (split[1].equals(CommonSetting.DATA_EXT)) {
                        String s = split[0];
                        String[] ss = s.split("_");
                        Integer i = Integer.parseInt(ss[1]);
                        dpFileMap.put(i, listFile);
                    }
                }
            }
        }
    }

    public void createFile(String tableName) throws IOException {
        String absolutePath = dataPath.getAbsolutePath();
        String p = absolutePath + File.separator + tableName;
        File tablePath = new File(p);
        if (!tablePath.exists()) {
            tablePath.mkdir();
        }
        for (int i = 0; i < CommonSetting.DATA_FILE_COUNT; i++) {
            String dp = p + File.separator + CommonSetting.DATA_NAME + "_" + i + CommonSetting.DOT + CommonSetting.DATA_EXT;
            File dpFile = new File(dp);
            if (!dpFile.exists()) {
                dpFile.createNewFile();
            }
            this.dpFileMap.put(i, dpFile);

            String ip = p + File.separator + CommonSetting.INDEX_NAME + "_" + i + CommonSetting.DOT + CommonSetting.INDEX_EXT;
            File ipFile = new File(ip);
            if (!ipFile.exists()) {
                ipFile.createNewFile();
            }
            this.ipFileMap.put(i, ipFile);
        }
    }

    public boolean hasFiles() {
        return dpFileMap.size() > 0 && ipFileMap.size() > 0;
    }

    public Map<Integer, File> getDpFileMap() {
        return dpFileMap;
    }

    public Map<Integer, File> getIpFileMap() {
        return ipFileMap;
    }
}
