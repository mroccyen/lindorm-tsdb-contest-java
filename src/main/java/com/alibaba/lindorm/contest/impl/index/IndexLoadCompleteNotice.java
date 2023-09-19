package com.alibaba.lindorm.contest.impl.index;

public class IndexLoadCompleteNotice {
    private boolean complete;
    private String tableName;
    private byte[] vin;
    private Index index;

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

    public byte[] getVin() {
        return vin;
    }

    public void setVin(byte[] vin) {
        this.vin = vin;
    }

    public Index getIndex() {
        return index;
    }

    public void setIndex(Index index) {
        this.index = index;
    }
}

