package com.alibaba.lindorm.contest.impl;

public class IndexBlock {
    private int position;
    private short tableNameLength;
    private byte[] tableName;
    private short rowKeyLength;
    private byte[] rowKey;

    public short getIndexBlockLength() {
        return (short) (4 + 2 + tableName.length + 2 + rowKey.length);
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
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
