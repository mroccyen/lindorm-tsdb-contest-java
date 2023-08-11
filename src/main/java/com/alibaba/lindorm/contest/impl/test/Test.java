package com.alibaba.lindorm.contest.impl.test;

import com.alibaba.lindorm.contest.impl.store.ByteBuffersDataInput;
import com.alibaba.lindorm.contest.impl.store.ByteBuffersDataOutput;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException {
        ByteBuffersDataOutput byteBuffersDataOutput = new ByteBuffersDataOutput();
        byteBuffersDataOutput.writeString("hello world...");
        ByteBuffersDataInput dataInput = byteBuffersDataOutput.toDataInput();
        System.out.println(dataInput.readString());
    }
}
