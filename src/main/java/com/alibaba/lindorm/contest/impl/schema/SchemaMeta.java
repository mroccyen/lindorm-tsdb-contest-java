package com.alibaba.lindorm.contest.impl.schema;

import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.util.ArrayList;

public class SchemaMeta {
    private int columnsNum = 0;
    private ArrayList<String> columnsName = new ArrayList<>();
    private ArrayList<ColumnValue.ColumnType> columnsType = new ArrayList<>();

    public int getColumnsNum() {
        return columnsNum;
    }

    public void setColumnsNum(int columnsNum) {
        this.columnsNum = columnsNum;
    }

    public ArrayList<String> getColumnsName() {
        return columnsName;
    }

    public void setColumnsName(ArrayList<String> columnsName) {
        this.columnsName = columnsName;
    }

    public ArrayList<ColumnValue.ColumnType> getColumnsType() {
        return columnsType;
    }

    public void setColumnsType(ArrayList<ColumnValue.ColumnType> columnsType) {
        this.columnsType = columnsType;
    }
}
