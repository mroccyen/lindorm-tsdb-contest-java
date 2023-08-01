package com.alibaba.lindorm.contest.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FileManager {
    private final Map<String, Map<Integer, FilePear>> fileMap = new HashMap<>();
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
                Map<Integer, FilePear> map = new HashMap<>(listFiles.length);
                for (File listFile : listFiles) {
                    String fileName = listFile.getName();
                    String[] split = fileName.split("\\.");
                    FilePear filePear = new FilePear();
                    int i = 0;
                    if (split[1].equals(CommonSetting.INDEX_EXT)) {
                        String s = split[0];
                        String[] ss = s.split("_");
                        i = Integer.parseInt(ss[1]);
                        filePear.setIpFile(listFile);
                    }
                    if (split[1].equals(CommonSetting.DATA_EXT)) {
                        String s = split[0];
                        String[] ss = s.split("_");
                        i = Integer.parseInt(ss[1]);
                        filePear.setDpFile(listFile);
                    }
                    map.put(i, filePear);
                }
                fileMap.put(tableName, map);
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
        Map<Integer, FilePear> map = new HashMap<>(CommonSetting.DATA_FILE_COUNT);
        for (int i = 0; i < CommonSetting.DATA_FILE_COUNT; i++) {
            FilePear filePear = new FilePear();

            String dp = p + File.separator + CommonSetting.DATA_NAME + "_" + i + CommonSetting.DOT + CommonSetting.DATA_EXT;
            File dpFile = new File(dp);
            if (!dpFile.exists()) {
                dpFile.createNewFile();
            }
            filePear.setDpFile(dpFile);

            String ip = p + File.separator + CommonSetting.INDEX_NAME + "_" + i + CommonSetting.DOT + CommonSetting.INDEX_EXT;
            File ipFile = new File(ip);
            if (!ipFile.exists()) {
                ipFile.createNewFile();
            }
            filePear.setIpFile(ipFile);

            map.put(i, filePear);
        }
        fileMap.put(tableName, map);
    }

    public boolean hasFiles() {
        return fileMap.size() > 0;
    }

    public Map<String, Map<Integer, FilePear>> getFileMap() {
        return fileMap;
    }

    public static class FilePear {
        private File ipFile;
        private File dpFile;

        public File getIpFile() {
            return ipFile;
        }

        public void setIpFile(File ipFile) {
            this.ipFile = ipFile;
        }

        public File getDpFile() {
            return dpFile;
        }

        public void setDpFile(File dpFile) {
            this.dpFile = dpFile;
        }
    }
}
