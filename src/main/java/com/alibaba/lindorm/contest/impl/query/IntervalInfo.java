package com.alibaba.lindorm.contest.impl.query;

public class IntervalInfo {
    private long timeLowerBound;
    private long timeUpperBound;
    private int maxInt = Integer.MIN_VALUE;
    private int totalInt = 0;
    private int totalCountInt = 0;
    private double maxDouble = Double.MIN_VALUE;
    private double totalDouble = 0;
    private int totalCountDouble = 0;

    public long getTimeLowerBound() {
        return timeLowerBound;
    }

    public void setTimeLowerBound(long timeLowerBound) {
        this.timeLowerBound = timeLowerBound;
    }

    public long getTimeUpperBound() {
        return timeUpperBound;
    }

    public void setTimeUpperBound(long timeUpperBound) {
        this.timeUpperBound = timeUpperBound;
    }

    public int getMaxInt() {
        return maxInt;
    }

    public void setMaxInt(int maxInt) {
        this.maxInt = maxInt;
    }

    public int getTotalInt() {
        return totalInt;
    }

    public void setTotalInt(int totalInt) {
        this.totalInt = totalInt;
    }

    public int getTotalCountInt() {
        return totalCountInt;
    }

    public void setTotalCountInt(int totalCountInt) {
        this.totalCountInt = totalCountInt;
    }

    public double getMaxDouble() {
        return maxDouble;
    }

    public void setMaxDouble(double maxDouble) {
        this.maxDouble = maxDouble;
    }

    public double getTotalDouble() {
        return totalDouble;
    }

    public void setTotalDouble(double totalDouble) {
        this.totalDouble = totalDouble;
    }

    public int getTotalCountDouble() {
        return totalCountDouble;
    }

    public void setTotalCountDouble(int totalCountDouble) {
        this.totalCountDouble = totalCountDouble;
    }
}
