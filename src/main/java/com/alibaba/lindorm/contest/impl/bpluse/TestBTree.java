package com.alibaba.lindorm.contest.impl.bpluse;

import java.util.Arrays;
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

        BTree<Integer> bt = new BTree<>(30);
        Integer[] arr = new Integer[3000000];
        for (int i = 0; i < arr.length; ++i) {
            arr[i] = i;
        }

        for (Integer s : arr) {
            bt.insert(s, s + "null");
        }
        bt.insert(150000, 150002 + "null");

        Result result = bt.searchKey(150000);
        Object[] keys = result.pt.keys;
        BTNode next = result.pt.next;
        Object[] keys1 = next.keys;
        next = next.next;

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
