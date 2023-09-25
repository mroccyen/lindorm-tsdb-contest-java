package com.alibaba.lindorm.contest.impl.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class ByteBufferUtils {
    public static List<ByteBuffer> splitByteBuffer(FileChannel fileChannel) throws IOException {
        long fileSize = fileChannel.size();
        if (fileSize == 0) {
            return new ArrayList<>();
        }
        int max = 1 << 30;
        int bufferCount = (int) Math.ceil((double) fileSize / (double) max);
        List<ByteBuffer> list = new ArrayList<>(bufferCount);

        long preLength = 0;
        long regionSize = max;
        for (int i = 0; i < bufferCount; i++) {
            if (fileSize - preLength < max) {
                regionSize = fileSize - preLength;
            }
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, preLength, regionSize);
            list.add(mappedByteBuffer);
        }
        return list;
    }
}
