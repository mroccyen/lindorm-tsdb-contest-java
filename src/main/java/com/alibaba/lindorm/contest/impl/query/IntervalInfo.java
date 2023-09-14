package com.alibaba.lindorm.contest.impl.query;

import com.alibaba.lindorm.contest.impl.common.CommonSetting;

import java.math.BigDecimal;

public class IntervalInfo {
    private boolean hasScanData = false;
    private boolean hasMaxInt = false;
    private boolean hasMaxDouble = false;
    private long timeLowerBound;
    private long timeUpperBound;
    private int maxInt = CommonSetting.INT_MIN;
    private BigDecimal totalInt = BigDecimal.ZERO;
    private BigDecimal totalCountInt = BigDecimal.ZERO;
    private double maxDouble = CommonSetting.DOUBLE_MIN;
    private BigDecimal totalDouble = BigDecimal.ZERO;
    private BigDecimal totalCountDouble = BigDecimal.ZERO;

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

    public BigDecimal getTotalInt() {
        return totalInt;
    }

    public void setTotalInt(BigDecimal totalInt) {
        this.totalInt = totalInt;
    }

    public BigDecimal getTotalCountInt() {
        return totalCountInt;
    }

    public void setTotalCountInt(BigDecimal totalCountInt) {
        this.totalCountInt = totalCountInt;
    }

    public double getMaxDouble() {
        return maxDouble;
    }

    public void setMaxDouble(double maxDouble) {
        this.maxDouble = maxDouble;
    }

    public BigDecimal getTotalDouble() {
        return totalDouble;
    }

    public void setTotalDouble(BigDecimal totalDouble) {
        this.totalDouble = totalDouble;
    }

    public BigDecimal getTotalCountDouble() {
        return totalCountDouble;
    }

    public void setTotalCountDouble(BigDecimal totalCountDouble) {
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
