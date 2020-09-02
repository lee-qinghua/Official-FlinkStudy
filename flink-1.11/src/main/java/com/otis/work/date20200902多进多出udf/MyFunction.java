package com.otis.work.date20200902多进多出udf;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 *
 */
class Result {
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

public class MyFunction extends AggregateFunction<String, Result> {

    @Override
    public String getValue(Result result) {
        //取出队列中的数据,计算各个指标,最后拼接成一个字符串返回
        LinkedList<Integer> list = result.queue;
        //当前值
        Integer cur_value = list.peekFirst();

        //把所有的值放到一个集合中
        List<Integer> values = new ArrayList<>();
        int sum = 0;
        for (Integer value : values) {
            sum += value;
        }
        StringBuilder builder = new StringBuilder();
        builder.append(sum).append(result.queue.peekFirst());
        return builder.toString();
    }

    @Override
    public Result createAccumulator() {
        Result result = new Result();
        result.queue = new LinkedList<Integer>();
        return result;
    }

    public void accumulate(Result acc, int value, int defaultValue) {
        //设置默认值
        acc.defaultValue = defaultValue;

        int queeSize = acc.queue.size();

        if (queeSize < 10) {
            acc.queue.addFirst(value);
        } else {
            acc.queue.pollLast();
            acc.queue.addFirst(value);
        }

    }

}
