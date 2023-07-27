package com.alibaba.lindorm.contest.impl.bpluse;

import java.util.Arrays;

public class BTNode {
    final int m; // m阶B树
    int size; // 当前ptr的数量
    BTNode next; // 下一个叶子结点
    BTNode prev; // 上一个叶子结点
    final Object[] keys; // 关键字数组
    final Object[] ptrs; // 指向子树(数据)的指针数组
    boolean isleaf; // 是否是叶子节点

    BTNode(int m) {
        this.m = m;
        keys = new Object[m + 1];
        ptrs = new Object[m + 1];
        size = 0;
        prev = next = null;
        isleaf = true;
    }

    @Override
    public String toString() {

        return String.format("Node_%s: %d%s", hashCode(), size, Arrays.toString(keys));
    }

    Object getMaxKey() {
        return keys[size - 1];
    }

    void setMaxKey(Object key) {
        keys[size - 1] = key;
    }

    public Object[] getKeys() {
        return keys;
    }

    public Object[] getPtrs() {
        return ptrs;
    }
}
