package com.alibaba.lindorm.contest.impl.query;

import com.alibaba.lindorm.contest.impl.common.CommonSetting;

public class IntervalInfo {
    private boolean hasScanData = false;
    private boolean hasMaxInt = false;
    private boolean hasMaxDouble = false;
    private long timeLowerBound;
    private long timeUpperBound;
    private int maxInt = CommonSetting.INT_MIN;
    private int totalInt = 0;
    private int totalCountInt = 0;
    private double maxDouble = CommonSetting.DOUBLE_MIN;
    private double totalDouble = 0;
    private int totalCountDouble = 0;

    public boolean hasScanData() {
        return hasScanData;
    }

    public void setHasScanData(boolean hasScanData) {
        this.hasScanData = hasScanData;
    }

    public boolean hasMaxInt() {
        return hasMaxInt;
    }

    public void setHasMaxInt(boolean hasMaxInt) {
        this.hasMaxInt = hasMaxInt;
    }

    public boolean hasMaxDouble() {
        return hasMaxDouble;
    }

    public void setHasMaxDouble(boolean hasMaxDouble) {
        this.hasMaxDouble = hasMaxDouble;
    }

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

    @Override
    public String toString() {
        return "IntervalInfo{" +
            "hasScanData=" + hasScanData +
            ", hasMaxInt=" + hasMaxInt +
            ", hasMaxDouble=" + hasMaxDouble +
            ", timeLowerBound=" + timeLowerBound +
            ", timeUpperBound=" + timeUpperBound +
            ", maxInt=" + maxInt +
            ", totalInt=" + totalInt +
            ", totalCountInt=" + totalCountInt +
            ", maxDouble=" + maxDouble +
            ", totalDouble=" + totalDouble +
            ", totalCountDouble=" + totalCountDouble +
            '}';
    }
}
