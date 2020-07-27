package com.otis.udfs;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.LinkedList;

public class DemoUdf extends AggregateFunction<Integer, PreviousValue> {
    @Override
    public Integer getValue(PreviousValue accumulator) {
        return  accumulator.queue.removeLast();
    }

    @Override
    public PreviousValue createAccumulator() {
        PreviousValue value = new PreviousValue();
        value.defaultValue = null;
        value.queue = new LinkedList<Integer>();
        value.length = 0;
        return value;
    }

    public void accumulate(PreviousValue acc, int value, int length, int defaultValue) {
        acc.length = length;
        acc.defaultValue = defaultValue;
        acc.queue.addFirst(value);

        int size = acc.queue.size();
        if (size < length) {
            for (int i = size; i < length; i++) {
                acc.queue.add(defaultValue);
            }
        } else if (size > length) {
            throw new RuntimeException("队列的大小出错了！！");
        }
    }
}