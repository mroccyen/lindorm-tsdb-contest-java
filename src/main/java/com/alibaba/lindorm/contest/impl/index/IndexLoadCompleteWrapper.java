package com.alibaba.lindorm.contest.impl.index;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class IndexLoadCompleteWrapper {
    private ReentrantLock lock;
    private Condition condition;

    public IndexLoadCompleteWrapper() {
        this.lock = new ReentrantLock();
        this.condition = this.lock.newCondition();
    }

    public ReentrantLock getLock() {
        return lock;
    }

    public void setLock(ReentrantLock lock) {
        this.lock = lock;
    }

    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }
}
