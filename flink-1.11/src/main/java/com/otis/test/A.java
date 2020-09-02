package com.otis.test;

import java.util.LinkedList;

public class A {
    public static void main(String[] args) {
        LinkedList<Integer> list = new LinkedList<>();
        list.addFirst(1);
        list.addFirst(2);
        System.out.println(list.size());

        System.out.println(list.peek());
        System.out.println(list.peek());


//        //返回但不删除第一个元素
//        //如果队列为空返回null
//        Integer first_value = list.peekFirst();
//
//        //返回并删除第一个元素
//        list.pollFirst();


    }
}
