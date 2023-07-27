package com.alibaba.lindorm.contest.impl.bpluse;

public class Result {
    BTNode pt;
    int index;
    boolean tag;

    public Result(BTNode pt, int index, boolean tag) {
        this.pt = pt;
        this.index = index;
        this.tag = tag;
    }

    public BTNode getPt() {
        return pt;
    }

    public int getIndex() {
        return index;
    }

    public boolean isTag() {
        return tag;
    }
}
