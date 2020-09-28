package com.otis.work.date20200902udf;

import java.util.LinkedList;

public class Result {
    int[] arr = {1, 2, 3};
    public LinkedList<Integer> queue;
    public Integer defaultValue;

    public Result() {
    }

    public Result(LinkedList<Integer> queue, Integer defaultValue) {
        this.queue = queue;
        this.defaultValue = defaultValue;
    }

    public LinkedList<Integer> getQueue() {
        return queue;
    }

    public void setQueue(LinkedList<Integer> queue) {
        this.queue = queue;
    }

    public Integer getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Integer defaultValue) {
        this.defaultValue = defaultValue;
    }
}
