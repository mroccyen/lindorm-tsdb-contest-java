package com.alibaba.lindorm.contest.impl.bpluse;

import java.util.List;
import java.util.Random;

public class TestBTree {
    public static void main(String[] args) {
//        BTree<String> bt = new BTree<>(30);
//        String[] arr = new String[3000];
//        for (int i = 0; i < arr.length; ++i) {
//            arr[i] = i + "";
//        }
//
//        for (String s : arr) {
//            bt.insert(s, "null");
//        }
//
//        Result result = bt.searchKey("15");
//        System.out.println("------: " + result.pt.keys);
//        shuffle(arr);
//
//        System.out.println(Arrays.toString(arr));
//        for (String s : arr) {
//            bt.delete(s + "");
//        }
//        bt.print();
//        bt.printKeysInorder();
//        for (String s : arr) {
//            bt.insert(s, "null");
//        }
//        shuffle(arr);
//
//        System.out.println(Arrays.toString(arr));
//        for (String s : arr) {
//            bt.delete(s + "");
//        }
//        bt.print();
//        bt.printKeysInorder();

        BTree<Long> bt = new BTree<>(30);
        Long[] arr = new Long[3000000];
        for (int i = 0; i < arr.length; ++i) {
            arr[i] = (long) i;
        }

        for (Long s : arr) {
            bt.insert(s, s + "null");
        }
        bt.delete(150001L);
        bt.delete(150002L);
        bt.delete(150003L);

        List<Object> list = bt.searchRange(1500001L, 150001L);
        Object o = bt.searchMax(Long.MAX_VALUE);
        int a = 0;
    }

    public static void shuffle(Comparable<?>[] nums) {
        Random rnd = new Random();
        for (int i = nums.length - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            //swap index i, j
            Comparable<?> t = nums[i];
            nums[i] = nums[j];
            nums[j] = t;
        }
    }
}
