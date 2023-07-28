package com.alibaba.lindorm.contest.impl;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class FileManager {
    private final Map<Integer, File> dpFileMap;
    private final Map<Integer, File> ipFileMap;

    public FileManager(File dataPath) throws IOException {
        dpFileMap = new TreeMap<>();
        ipFileMap = new TreeMap<>();

        String absolutePath = dataPath.getAbsolutePath();
        for (int i = 0; i < CommonSetting.DATA_FILE_COUNT; i++) {
            String dp = absolutePath + File.separator + CommonSetting.DATA_NAME + "_" + i + CommonSetting.DATA_EXT;
            File dpFile = new File(dp);
            if (!dpFile.exists()) {
                dpFile.createNewFile();
            }
            this.dpFileMap.put(i, dpFile);

            String ip = absolutePath + File.separator + CommonSetting.INDEX_NAME + "_" + i + CommonSetting.INDEX_EXT;
            File ipFile = new File(ip);
            if (!ipFile.exists()) {
                ipFile.createNewFile();
            }
            this.ipFileMap.put(i, ipFile);
        }
    }

    public Map<Integer, File> getDpFileMap() {
        return dpFileMap;
    }

    public Map<Integer, File> getIpFileMap() {
        return ipFileMap;
    }
}
