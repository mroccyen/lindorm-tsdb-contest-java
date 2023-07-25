package com.alibaba.lindorm.contest.impl;

public class IndexLoadCompleteNotice {
    private boolean complete;
    private byte[] indexDataByte;

    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    public byte[] getIndexDataByte() {
        return indexDataByte;
    }

    public void setIndexDataByte(byte[] indexDataByte) {
        this.indexDataByte = indexDataByte;
    }
}
