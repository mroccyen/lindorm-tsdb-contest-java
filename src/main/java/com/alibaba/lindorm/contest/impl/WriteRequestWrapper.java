package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.WriteRequest;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class WriteRequestWrapper {
    private WriteRequest writeRequest;

    private ReentrantLock lock;

    private Condition condition;

    public WriteRequestWrapper() {
        this.lock = new ReentrantLock();
        this.condition = this.lock.newCondition();
    }

    public void bindRequest(WriteRequest writeRequest) {
        this.writeRequest = writeRequest;
    }

    public WriteRequest getWriteRequest() {
        return writeRequest;
    }

    public ReentrantLock getLock() {
        return lock;
    }

    public Condition getCondition() {
        return condition;
    }
}
