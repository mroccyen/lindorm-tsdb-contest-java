package com.alibaba.lindorm.contest.impl.utils;

public class ByteArrayUtil {
    /**
     * 字节数组转 short，小端
     */
    public static short byteArray2Short_Little_Endian(byte[] array) {

        // 数组长度有误
        if (array.length > 2) {
            return 0;
        }

        short value = 0;
        for (int i = 0; i < array.length; i++) {
            // & 0xff，除去符号位干扰
            value |= ((array[i] & 0xff) << (i * 8));
        }
        return value;
    }

    /**
     * 字节数组转 short，大端
     */
    public static short byteArray2Short_Big_Endian(byte[] array) {

        // 数组长度有误
        if (array.length > 2) {
            return 0;
        }

        short value = 0;
        for (int i = 0; i < array.length; i++) {
            value |= ((array[i] & 0xff) << ((array.length - i - 1) * 8));
        }
        return value;
    }

    /**
     * 字节数组转 int，小端
     */
    public static int byteArray2Int_Little_Endian(byte[] array) {

        // 数组长度有误
        if (array.length > 4) {
            return 0;
        }

        int value = 0;
        for (int i = 0; i < array.length; i++) {
            value |= ((array[i] & 0xff) << (i * 8));

        }
        return value;
    }

    /**
     * 字节数组转 int，大端
     */
    public static int byteArray2Int_Big_Endian(byte[] array) {

        // 数组长度有误
        if (array.length > 4) {
            return 0;
        }

        int value = 0;
        for (int i = 0; i < array.length; i++) {
            value |= ((array[i] & 0xff) << ((array.length - i - 1) * 8));
        }
        return value;
    }

    /**
     * 字节数组转 float，小端
     */
    public static float byteArray2Float_Little_Endian(byte[] array) {

        // 数组长度有误
        if (array.length != 4) {
            return 0;
        }

        return Float.intBitsToFloat(byteArray2Int_Little_Endian(array));
    }

    /**
     * 字节数组转 float，大端
     */
    public static float byteArray2Float_Big_Endian(byte[] array) {

        // 数组长度有误
        if (array.length > 4) {
            return 0;
        }

        return Float.intBitsToFloat(byteArray2Int_Big_Endian(array));
    }

    /**
     * 字节数组转 long，小端
     */
    public static long byteArray2Long_Little_Endian(byte[] array) {

        // 数组长度有误
        if (array.length != 8) {
            return 0;
        }

        long value = 0;
        for (int i = 0; i < array.length; i++) {
            // 需要转long再位移，否则int丢失精度
            value |= ((long) (array[i] & 0xff) << (i * 8));
        }
        return value;
    }

    /**
     * 字节数组转 long，大端
     */
    public static long byteArray2Long_Big_Endian(byte[] array) {

        // 数组长度有误
        if (array.length != 8) {
            return 0;
        }

        long value = 0;
        for (int i = 0; i < array.length; i++) {
            value |= ((long) (array[i] & 0xff) << ((array.length - i - 1) * 8));
        }
        return value;
    }

    /**
     * 字节数组转 double，小端
     */
    public static double byteArray2Double_Little_Endian(byte[] array) {

        // 数组长度有误
        if (array.length != 8) {
            return 0;
        }

        return Double.longBitsToDouble(byteArray2Long_Little_Endian(array));
    }

    /**
     * 字节数组转 double，大端
     */
    public static double byteArray2Double_Big_Endian(byte[] array) {

        // 数组长度有误
        if (array.length != 8) {
            return 0;
        }

        return Double.longBitsToDouble(byteArray2Long_Big_Endian(array));
    }

    /**
     * 字节数组转 HexString
     */
    public static String byteArray2HexString(byte[] array) {

        StringBuilder builder = new StringBuilder();
        for (byte b : array) {

            String s = Integer.toHexString(b & 0xff);
            if (s.length() < 2) {
                builder.append("0");
            }
            builder.append(s);
        }

        return builder.toString().toUpperCase();
    }

    //---------------------------------华丽的分割线-------------------------------------

    /**
     * short 转字节数组，小端
     */
    public static byte[] short2ByteArray_Little_Endian(short s) {

        byte[] array = new byte[2];

        for (int i = 0; i < array.length; i++) {
            array[i] = (byte) (s >> (i * 8));
        }
        return array;
    }

    /**
     * short 转字节数组，大端
     */
    public static byte[] short2ByteArray_Big_Endian(short s) {

        byte[] array = new byte[2];

        for (int i = 0; i < array.length; i++) {
            array[array.length - 1 - i] = (byte) (s >> (i * 8));
        }
        return array;
    }

    /**
     * int 转字节数组，小端
     */
    public static byte[] int2ByteArray_Little_Endian(int s) {

        byte[] array = new byte[4];

        for (int i = 0; i < array.length; i++) {
            array[i] = (byte) (s >> (i * 8));
        }
        return array;
    }

    /**
     * int 转字节数组，大端
     */
    public static byte[] int2ByteArray_Big_Endian(int s) {

        byte[] array = new byte[4];

        for (int i = 0; i < array.length; i++) {
            array[array.length - 1 - i] = (byte) (s >> (i * 8));
        }
        return array;
    }

    /**
     * float 转字节数组，小端
     */
    public static byte[] float2ByteArray_Little_Endian(float f) {

        return int2ByteArray_Little_Endian(Float.floatToIntBits(f));
    }

    /**
     * float 转字节数组，大端
     */
    public static byte[] float2ByteArray_Big_Endian(float f) {

        return int2ByteArray_Big_Endian(Float.floatToIntBits(f));
    }

    /**
     * long 转字节数组，小端
     */
    public static byte[] long2ByteArray_Little_Endian(long l) {

        byte[] array = new byte[8];

        for (int i = 0; i < array.length; i++) {
            array[i] = (byte) (l >> (i * 8));
        }
        return array;
    }

    /**
     * long 转字节数组，大端
     */
    public static byte[] long2ByteArray_Big_Endian(long l) {

        byte[] array = new byte[8];

        for (int i = 0; i < array.length; i++) {
            array[array.length - 1 - i] = (byte) (l >> (i * 8));
        }
        return array;
    }

    /**
     * double 转字节数组，小端
     */
    public static byte[] double2ByteArray_Little_Endian(double d) {

        return long2ByteArray_Little_Endian(Double.doubleToLongBits(d));
    }

    /**
     * double 转字节数组，大端
     */
    public static byte[] double2ByteArray_Big_Endian(double d) {

        return long2ByteArray_Big_Endian(Double.doubleToLongBits(d));
    }

    /**
     * HexString 转字节数组
     */
    public static byte[] hexString2ByteArray(String hexString) {

        // 两个十六进制字符一个 byte，单数则有误
        if (hexString.length() % 2 != 0) {
            return null;
        }

        byte[] array = new byte[hexString.length() / 2];

        int value = 0;
        for (int i = 0; i < hexString.length(); i++) {

            char s = hexString.charAt(i);

            // 前半个字节
            if (i % 2 == 0) {
                value = Integer.parseInt(String.valueOf(s), 16) * 16;
            } else {
                // 后半个字节
                value += Integer.parseInt(String.valueOf(s), 16);
                array[i / 2] = (byte) value;
                value = 0;
            }
        }

        return array;
    }
}
