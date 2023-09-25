package com.alibaba.lindorm.contest.impl.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class ByteBufferUtils {
    public static List<ByteBuffer> splitByteBuffer(FileChannel fileChannel) throws IOException {
        long fileSize = fileChannel.size();
        if (fileSize == 0) {
            return new ArrayList<>();
        }
        int bufferCount = (int) Math.ceil((double) fileSize / (double) Integer.MAX_VALUE);
        List<ByteBuffer> list = new ArrayList<>(bufferCount);

        long preLength = 0;
        long regionSize = Integer.MAX_VALUE;
        for (int i = 0; i < bufferCount; i++) {
            if (fileSize - preLength < Integer.MAX_VALUE) {
                regionSize = fileSize - preLength;
            }
            list.add(fileChannel.map(FileChannel.MapMode.READ_ONLY, preLength, regionSize));
        }
        return list;
    }
}
