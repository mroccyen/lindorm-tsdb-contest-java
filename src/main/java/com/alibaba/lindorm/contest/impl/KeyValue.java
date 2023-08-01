package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.nio.ByteBuffer;

public class KeyValue {
    private byte columnNameLength;
    private byte[] columnName;
    private ColumnValue.ColumnType columnType;
    private ColumnValue columnValue;

    public byte getColumnNameLength() {
        return columnNameLength;
    }

    public void setColumnNameLength(byte columnNameLength) {
        this.columnNameLength = columnNameLength;
    }

    public byte[] getColumnName() {
        return columnName;
    }

    public void setColumnName(byte[] columnName) {
        this.columnName = columnName;
    }

    public ColumnValue.ColumnType getColumnType() {
        return columnType;
    }

    public void setColumnType(ColumnValue.ColumnType columnType) {
        this.columnType = columnType;
    }

    public int getValueLength() {
        if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_STRING)) {
            return columnValue.getStringValue().remaining();
        }
        if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)) {
            return 4;
        }
        if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
            return 8;
        }
        return 0;
    }

    public ColumnValue getColumnValue() {
        return columnValue;
    }

    public void setColumnValue(ColumnValue columnValue) {
        this.columnValue = columnValue;
    }

    public byte getValueType() {
        if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_STRING)) {
            return 1;
        }
        if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)) {
            return 2;
        }
        if (columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
            return 3;
        }
        return 0;
    }

    public ByteBuffer getByteBufferValue() {
        ColumnValue.StringColumn stringColumn = (ColumnValue.StringColumn) columnValue;
        return stringColumn.getStringValue();
    }

    public int getIntegerValue() {
        ColumnValue.IntegerColumn integerColumn = (ColumnValue.IntegerColumn) columnValue;
        return integerColumn.getIntegerValue();
    }

    public double getDoubleValue() {
        ColumnValue.DoubleFloatColumn doubleFloatColumn = (ColumnValue.DoubleFloatColumn) columnValue;
        return doubleFloatColumn.getDoubleFloatValue();
    }
}
