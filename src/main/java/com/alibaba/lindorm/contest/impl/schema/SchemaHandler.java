package com.alibaba.lindorm.contest.impl.schema;

import com.alibaba.lindorm.contest.impl.common.CommonSetting;
import com.alibaba.lindorm.contest.impl.file.FileManager;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Schema;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class SchemaHandler {
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
            ColumnValue.ColumnType cType = entry.getValue();
            switch (cType) {
                case COLUMN_TYPE_INTEGER:
                    schemaMeta.getIntegerColumnsName().add(entry.getKey());
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    schemaMeta.getDoubleColumnsName().add(entry.getKey());
                    break;
                case COLUMN_TYPE_STRING:
                    schemaMeta.getStringColumnsName().add(entry.getKey());
                    break;
                default:
                    throw new IllegalStateException("Undefined column type, this is not expected");
            }
        }
        fileManager.addSchemaMeta(tableName, schemaMeta);
    }

    public void loadTableInfo() throws IOException {
        File schemaFile = new File(dataPath, CommonSetting.SCHEMA_FILE);
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
                    String columnName = parts[index++];
                    ColumnValue.ColumnType columnType = ColumnValue.ColumnType.valueOf(parts[index++]);
                    switch (columnType) {
                        case COLUMN_TYPE_INTEGER:
                            schemaMeta.getIntegerColumnsName().add(columnName);
                            break;
                        case COLUMN_TYPE_DOUBLE_FLOAT:
                            schemaMeta.getDoubleColumnsName().add(columnName);
                            break;
                        case COLUMN_TYPE_STRING:
                            schemaMeta.getStringColumnsName().add(columnName);
                            break;
                        default:
                            throw new IllegalStateException("Undefined column type, this is not expected");
                    }
                }
                fileManager.addSchemaMeta(tableName, schemaMeta);
            }
        }
    }

    public void flushTableInfo() {
        try {
            File schemaFile = new File(dataPath, CommonSetting.SCHEMA_FILE);
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
        for (int i = 0; i < schemaMeta.getIntegerColumnsName().size(); ++i) {
            sb.append(",")
                .append(schemaMeta.getIntegerColumnsName().get(i))
                .append(",")
                .append(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
        }
        for (int i = 0; i < schemaMeta.getDoubleColumnsName().size(); ++i) {
            sb.append(",")
                .append(schemaMeta.getDoubleColumnsName().get(i))
                .append(",")
                .append(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
        }
        for (int i = 0; i < schemaMeta.getStringColumnsName().size(); ++i) {
            sb.append(",")
                .append(schemaMeta.getStringColumnsName().get(i))
                .append(",")
                .append(ColumnValue.ColumnType.COLUMN_TYPE_STRING);
        }
        return sb.toString();
    }
}
