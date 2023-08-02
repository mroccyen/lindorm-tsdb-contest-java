package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Schema;

import java.io.*;
import java.util.Map;

public class SchemaHandler {
    private static final String SCHEMA_FILE = "schema.txt";
    private final FileManager fileManager;
    private final File dataPath;

    public SchemaHandler(FileManager fileManager, File dataPath) {
        this.fileManager = fileManager;
        this.dataPath = dataPath;
    }

    public void cacheTableInfo(String tableName, Schema schema) {
        Map<String, ColumnValue.ColumnType> columnTypeMap = schema.getColumnTypeMap();
        SchemaMeta schemaMeta = new SchemaMeta();
        schemaMeta.setColumnsNum(columnTypeMap.size());
        for (Map.Entry<String, ColumnValue.ColumnType> entry : columnTypeMap.entrySet()) {
            schemaMeta.getColumnsName().add(entry.getKey());
            schemaMeta.getColumnsType().add(entry.getValue());
        }
        fileManager.addSchemaMeta(tableName, schemaMeta);
    }

    public void loadTableInfo() throws IOException {
        File schemaFile = new File(dataPath, SCHEMA_FILE);
        if (!schemaFile.exists() || !schemaFile.isFile()) {
            System.out.println("Connect new database with empty pre-written data");
            return;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(schemaFile))) {
            String line;
            if ((line = reader.readLine()) != null && !line.isEmpty()) {
                String[] parts = line.split(",");
                String tableName = parts[0];
                SchemaMeta schemaMeta = new SchemaMeta();
                int columnsNum = Integer.parseInt(parts[1]);
                schemaMeta.setColumnsNum(columnsNum);
                if (columnsNum <= 0) {
                    System.err.println("Unexpected columns' num: [" + columnsNum + "]");
                    throw new RuntimeException();
                }
                int index = 2;
                for (int i = 0; i < columnsNum; i++) {
                    schemaMeta.getColumnsName().add(parts[index++]);
                    schemaMeta.getColumnsType().add(ColumnValue.ColumnType.valueOf(parts[index++]));
                }
                fileManager.addSchemaMeta(tableName, schemaMeta);
            }
        }
    }

    public void flushTableInfo() {
        try {
            File schemaFile = new File(dataPath, SCHEMA_FILE);
            schemaFile.delete();
            schemaFile.createNewFile();
            BufferedWriter writer = new BufferedWriter(new FileWriter(schemaFile));
            for (Map.Entry<String, SchemaMeta> e : fileManager.getTableSchemaMetaMap().entrySet()) {
                writer.write(schemaToString(e.getKey(), e.getValue()));
            }
            writer.close();
        } catch (IOException e) {
            System.err.println("Error saving the schema");
            throw new RuntimeException(e);
        }
    }

    private String schemaToString(String table, SchemaMeta schemaMeta) {
        StringBuilder sb = new StringBuilder();
        sb.append(table);
        sb.append(",");
        sb.append(schemaMeta.getColumnsNum());
        for (int i = 0; i < schemaMeta.getColumnsNum(); ++i) {
            sb.append(",")
                .append(schemaMeta.getColumnsName().get(i))
                .append(",")
                .append(schemaMeta.getColumnsType().get(i));
        }
        return sb.toString();
    }
}
