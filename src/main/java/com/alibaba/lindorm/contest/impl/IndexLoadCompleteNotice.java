package com.alibaba.lindorm.contest.impl;

public class IndexLoadCompleteNotice {
    private boolean complete;
    private String tableName;
    private byte[] indexDataByte;

    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public byte[] getIndexDataByte() {
        return indexDataByte;
    }

    public void setIndexDataByte(byte[] indexDataByte) {
        this.indexDataByte = indexDataByte;
    }
}
