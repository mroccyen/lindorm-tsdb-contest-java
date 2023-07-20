package com.alibaba.lindorm.contest.impl;

public class IndexBlock {
    private int offset;
    private short dataSize;
    private short tableNameLength;
    private byte[] tableName;
    private short rowKeyLength;
    private byte[] rowKey;

    public short getIndexBlockLength() {
        return (short) (4 + 2 + 2 + tableName.length + 2 + rowKey.length);
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public short getDataSize() {
        return dataSize;
    }

    public void setDataSize(short dataSize) {
        this.dataSize = dataSize;
    }

    public short getTableNameLength() {
        return tableNameLength;
    }

    public void setTableNameLength(short tableNameLength) {
        this.tableNameLength = tableNameLength;
    }

    public byte[] getTableName() {
        return tableName;
    }

    public void setTableName(byte[] tableName) {
        this.tableName = tableName;
    }

    public short getRowKeyLength() {
        return rowKeyLength;
    }

    public void setRowKeyLength(short rowKeyLength) {
        this.rowKeyLength = rowKeyLength;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }
}
