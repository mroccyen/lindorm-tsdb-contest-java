package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.ColumnValue;

public class QueryResult {
    private long timestamp;
    private String columnName;
    private ColumnValue columnValue;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public ColumnValue getColumnValue() {
        return columnValue;
    }

    public void setColumnValue(ColumnValue columnValue) {
        this.columnValue = columnValue;
    }
}
