package com.alibaba.lindorm.contest.impl.index;

public class IndexLoadCompleteNotice {
    private boolean complete;
    private String tableName;
    private long offset;
    private byte[] vin;

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

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public byte[] getVin() {
        return vin;
    }

    public void setVin(byte[] vin) {
        this.vin = vin;
    }
}
