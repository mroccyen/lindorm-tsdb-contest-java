package com.alibaba.lindorm.contest.impl;

public class IndexBlock {
    private long offset;
    private long timestamp;
    private byte index;
    private int dataSize;
    private int tableNameLength;
    private byte[] tableName;
    private int rowKeyLength;
    private byte[] rowKey;

    public int getIndexBlockLength() {
        return 8 + 8 + 1 + 4 + 4 + tableName.length + 4 + rowKey.length;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public byte getIndex() {
        return index;
    }

    public void setIndex(byte index) {
        this.index = index;
    }

    public int getDataSize() {
        return dataSize;
    }

    public void setDataSize(int dataSize) {
        this.dataSize = dataSize;
    }

    public int getTableNameLength() {
        return tableNameLength;
    }

    public void setTableNameLength(int tableNameLength) {
        this.tableNameLength = tableNameLength;
    }

    public byte[] getTableName() {
        return tableName;
    }

    public void setTableName(byte[] tableName) {
        this.tableName = tableName;
    }

    public int getRowKeyLength() {
        return rowKeyLength;
    }

    public void setRowKeyLength(int rowKeyLength) {
        this.rowKeyLength = rowKeyLength;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }
}
