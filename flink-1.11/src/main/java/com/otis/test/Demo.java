package com.otis.test;

import java.util.LinkedList;

public class Demo {
    public static <T> void main(String[] args) {
        LinkedList<Integer> list = new LinkedList<>();
        list.add(4);
        list.add(6);
        list.add(8);
        list.addFirst(10);
        System.out.println(list.getFirst());
        System.out.println(list.getLast());

    }
}
