package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.LatestQueryRequest;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.TimeRangeQueryRequest;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.READ;

public class QueryHandler {
    private final File dpFile;

    public QueryHandler(File dpFile) {
        this.dpFile = dpFile;
    }

    public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        String tableName = pReadReq.getTableName();
        List<IndexBlock> indexBlocks = IndexBufferHandler.getIndexBlocks(tableName);
        Map<String, List<IndexBlock>> rowKeyMap = indexBlocks.stream().collect(Collectors.groupingBy(i -> new String(i.getRowKey())));
        Collection<Vin> vinList = pReadReq.getVins();
        Set<String> requestedColumns = pReadReq.getRequestedColumns();
        for (Vin vin : vinList) {
            String rowKeyName = new String(vin.getVin());
            if (rowKeyMap.containsKey(rowKeyName)) {
                List<IndexBlock> indexBlockList = rowKeyMap.get(rowKeyName);
                for (IndexBlock indexBlock : indexBlockList) {
                    ByteBuffer sizeByteBuffer = ByteBuffer.allocateDirect(indexBlock.getDataSize());
                    FileChannel fileChannel = FileChannel.open(dpFile.toPath(), READ);
                    if (fileChannel.size() == 0) {
                        return new ArrayList<>();
                    }
                    fileChannel.read(sizeByteBuffer, indexBlock.getOffset());
                    sizeByteBuffer.flip();
                    int bufferPosition = sizeByteBuffer.position();
                    int bufferLimit = sizeByteBuffer.limit();
                    while (bufferPosition < bufferLimit) {
                        sizeByteBuffer.getShort();
                        short rowKeyLength = sizeByteBuffer.getShort();
                        byte[] rowKey = new byte[rowKeyLength];
                        for (short i = 0; i < rowKeyLength; i++) {
                            rowKey[i] = sizeByteBuffer.get();
                        }
                        String existRowKey = new String(rowKey);
                        short columnNameLength = sizeByteBuffer.getShort();
                        byte[] columnName = new byte[columnNameLength];
                        for (short i = 0; i < columnNameLength; i++) {
                            columnName[i] = sizeByteBuffer.get();
                        }
                        String existColumnName = new String(columnName);
                        long timestamp = sizeByteBuffer.getLong();
                        byte valueType = sizeByteBuffer.get();
                        short valueLength = sizeByteBuffer.getShort();
                        if (valueType == 1) {
                            byte[] valueBytes = new byte[valueLength];
                            for (short i = 0; i < valueLength; i++) {
                                valueBytes[i] = sizeByteBuffer.get();
                            }
                            String value = new String(valueBytes);
                            if (existRowKey.equals(rowKeyName) && requestedColumns.contains(existColumnName)) {

                            }
                        }
                        if (valueType == 2) {
                            int value = sizeByteBuffer.getInt();
                        }
                        if (valueType == 3) {
                            double value = sizeByteBuffer.getDouble();
                        }
                        bufferPosition = sizeByteBuffer.position();
                        bufferLimit = sizeByteBuffer.limit();
                    }
                    sizeByteBuffer = null;
                    fileChannel.close();
                }
            }
        }
        return new ArrayList<>();
    }

    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        return new ArrayList<>();
    }
}
