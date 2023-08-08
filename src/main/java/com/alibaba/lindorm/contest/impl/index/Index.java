package com.alibaba.lindorm.contest.impl.index;

public class Index {
    private long offset;
    private byte[] rowKey;
    private long latestTimestamp;

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public long getLatestTimestamp() {
        return latestTimestamp;
    }

    public void setLatestTimestamp(long latestTimestamp) {
        this.latestTimestamp = latestTimestamp;
    }
}
