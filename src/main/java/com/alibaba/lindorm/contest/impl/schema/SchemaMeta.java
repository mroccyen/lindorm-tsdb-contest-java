package com.alibaba.lindorm.contest.impl.schema;

import java.util.ArrayList;

public class SchemaMeta {
    private int columnsNum = 0;
    private ArrayList<String> stringColumnsName = new ArrayList<>();
    private ArrayList<String> integerColumnsName = new ArrayList<>();
    private ArrayList<String> doubleColumnsName = new ArrayList<>();

    public int getColumnsNum() {
        return columnsNum;
    }

    public void setColumnsNum(int columnsNum) {
        this.columnsNum = columnsNum;
    }

    public ArrayList<String> getStringColumnsName() {
        return stringColumnsName;
    }

    public void setStringColumnsName(ArrayList<String> stringColumnsName) {
        this.stringColumnsName = stringColumnsName;
    }

    public ArrayList<String> getIntegerColumnsName() {
        return integerColumnsName;
    }

    public void setIntegerColumnsName(ArrayList<String> integerColumnsName) {
        this.integerColumnsName = integerColumnsName;
    }

    public ArrayList<String> getDoubleColumnsName() {
        return doubleColumnsName;
    }

    public void setDoubleColumnsName(ArrayList<String> doubleColumnsName) {
        this.doubleColumnsName = doubleColumnsName;
    }
}
