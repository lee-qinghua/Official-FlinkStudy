package com.otis.work.date20200902udf;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.LinkedList;


/**
 * Previous aggregate function.
 */
public abstract class PreviousAvgValueFunction<T> extends AggregateFunction<Double, PreviousAvgValueFunction.PreviousAvgValue<Double>> {

    public static class PreviousAvgValue<Double> {
        public LinkedList<Double> queue;
        public int length;
        public Object defaultValue;
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

    @Override
    public PreviousAvgValue<Double> createAccumulator() {
        PreviousAvgValue<Double> acc = new PreviousAvgValue();
        acc.queue = new LinkedList<Double>();
        acc.length = 0;
        acc.defaultValue = null;
        return acc;
    }

    public void accumulate(PreviousAvgValue<Double> acc, Object value, int length, Object defaultValue) {
        acc.length = length;
        acc.defaultValue = defaultValue;
        int accSize = acc.queue.size();
        //队列填充默认值

        if(accSize < length) {
            if (value != null) {
                acc.queue.addFirst((Double) value);
            } else {
                acc.queue.addFirst((Double) defaultValue);
            }
        }else{
            acc.queue.removeLast();
            if (value != null) {
                acc.queue.addFirst((Double) value);
            } else {
                acc.queue.addFirst((Double) defaultValue);
            }
        }
    }


    public void accumulate(PreviousAvgValue<Double> acc, Object value, int length) {
        acc.length = length;
        acc.defaultValue = value;

        int accSize = acc.queue.size();
        if (accSize < length) {
            for (int i = 0; i < length - accSize; i++) {

                acc.queue.addFirst((Double) value);
            }
        }
        acc.queue.addFirst((Double) value);
    }



    @Override
    public Double getValue(PreviousAvgValue<Double> acc) {
        int accSize = acc.queue.size();
        double sum = 0.0;

        if (accSize < acc.length) {
            return (Double) (acc.defaultValue);
        } else {
            for(Double value:acc.queue){
                sum += value;
            }
            return (sum/acc.length);
        }

    }
}