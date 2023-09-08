package com.alibaba.lindorm.contest.impl.common;

public class CommonSetting {
    public final static Integer WRITE_THREAD_COUNT = 8;
    public static final String SCHEMA_FILE = "schema.txt";
    public final static String LATEST_INDEX_FILE_NAME = "latest_index";
    /**
     * 2023-07-12 00:00:00
     */
    public final static long DEFAULT_TIMESTAMP = 1689091200000L;
    public final static int INT_NAN = 0x80000000;
    public final static double DOUBLE_NAN = 0xfff0000000000000L;
}
