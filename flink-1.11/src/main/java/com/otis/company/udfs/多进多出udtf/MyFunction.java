package com.otis.company.udfs.多进多出udtf;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * 这个类是中间累加计算的结果，里面包含
 * 最近10条数据的和
 * 最近2条数据的最大值
 * 最近5条数据的最小值
 */
class Result {
    public Integer result_sum;
    public Integer result_max;
    public Integer result_min;

    public Integer getResult_sum() {
        return result_sum;
    }

    public void setResult_sum(Integer result_sum) {
        this.result_sum = result_sum;
    }

    public Integer getResult_max() {
        return result_max;
    }

    public void setResult_max(Integer result_max) {
        this.result_max = result_max;
    }

    public Integer getResult_min() {
        return result_min;
    }

    public void setResult_min(Integer result_min) {
        this.result_min = result_min;
    }
}

public class MyFunction extends TableAggregateFunction<Tuple3<Integer, Integer, Integer>, Result> {
    @Override
    public Result createAccumulator() {
        Result result = new Result();
        result.result_sum = 0;
        result.result_min = Integer.MAX_VALUE;
        result.result_max = Integer.MIN_VALUE;
        return result;
    }


    /**
     * 每来一条数据都会触发计算一次
     *
     * @param acc   计算的中间结果
     * @param value 传过来的每一条数据
     */
    public void accumulate(Result acc, Integer value) {
        acc.result_sum += value;
        acc.result_min = Math.min(value, acc.result_min);
        acc.result_max = Math.max(value, acc.result_max);
    }

    /**
     * 对于流式处理多个分区合并
     *
     * @param acc
     * @param iterable
     */
    public void merge(Result acc, java.lang.Iterable<Result> iterable) {
        for (Result otherAcc : iterable) {
            acc.result_sum += otherAcc.result_sum;
            acc.result_min = Math.min(otherAcc.result_min, acc.result_min);
            acc.result_max = Math.max(otherAcc.result_max, acc.result_max);
        }
    }

    /**
     * 输出的是一个tuple3
     *
     * @param acc
     * @param out
     */
    public void emitValue(Result acc, Collector<Tuple3<Integer, Integer, Integer>> out) {
        // 输出tuple3 分别是：和，最小值，最大值
        out.collect(Tuple3.of(acc.result_sum, acc.result_min, acc.result_max));
    }
}
