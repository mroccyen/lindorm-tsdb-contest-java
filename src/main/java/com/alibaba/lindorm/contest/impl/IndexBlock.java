package com.alibaba.lindorm.contest.impl;

public class IndexBlock {
    private long offset;
    private long timestamp;
    private byte index;
    private int dataSize;
    private byte tableNameLength;
    private byte[] tableName;
    private byte[] rowKey;

    public byte getIndexBlockLength() {
        return (byte) (8 + 8 + 1 + 4 + 1 + tableName.length + rowKey.length);
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

    public byte getTableNameLength() {
        return tableNameLength;
    }

    public void setTableNameLength(byte tableNameLength) {
        this.tableNameLength = tableNameLength;
    }

    public byte[] getTableName() {
        return tableName;
    }

    public void setTableName(byte[] tableName) {
        this.tableName = tableName;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }
}
