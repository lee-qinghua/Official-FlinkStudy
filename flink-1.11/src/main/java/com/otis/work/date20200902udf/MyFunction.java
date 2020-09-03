package com.otis.work.date20200902udf;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.*;

public class MyFunction extends AggregateFunction<String, Result> {
    //求list中的平均值
    public double avgAmount(List<Integer> list) {
        double sum = 0;
        for (Integer value : list) {
            sum += value;
        }
        return (double) sum / list.size();
    }

    //求方差
    public double fangcha(List<Integer> list, int curValue) {
        int sum = 0;
        for (Integer value : list) {
            sum += value;
        }
        sum += curValue;
        double avg = (double) sum / (list.size() + 1);
        double fangchaSum = 0;
        for (Integer value : list) {
            fangchaSum += (value - avg) * (value - avg);
        }
        fangchaSum += (curValue - avg) * (curValue - avg);
        return Math.sqrt(fangchaSum / (list.size() + 1));
    }

    @Override
    public String getValue(Result result) {
        System.out.println("添加元素之后的队列长度为： " + result.queue.size());
//        for (Integer integer : result.queue) {
//            System.out.println("队列中的元素为：=====================" + integer);
//        }
        //取出队列中的数据,计算各个指标,最后拼接成一个字符串返回
        LinkedList<Integer> list = result.queue;
        //队列当前的长度
        int size = list.size();
        //当前值
        Integer cur_value = list.peekLast();
        //默认值
        Integer defaultValue = result.defaultValue;
        //拼接返回的结果
        StringBuilder sb = new StringBuilder();

        //===================================================================
        //                                                  todo 前2次相关
        //===================================================================
        List<Integer> two_list = new ArrayList<>();
        try {
            two_list.add(list.get(size - 2));
        } catch (Exception e) {
            two_list.add(defaultValue);
        }
        try {
            two_list.add(list.get(size - 3));
        } catch (Exception e) {
            two_list.add(defaultValue);
        }


        if (two_list.contains(defaultValue)) {
            for (int i = 0; i < 5; i++) {
                sb.append(defaultValue).append(",");
            }
        } else {
            //过去2次平均交易金额
            double two_a = avgAmount(two_list);
            sb.append(two_a).append(",");
            //当前交易金额与过去2次交易金额均值之比
            double two_b = cur_value / two_a;
            sb.append(two_b).append(",");
            //当前交易金额与过去2次交易金额最大值之比
            double two_c = (double) cur_value / Collections.max(two_list);
            sb.append(two_c).append(",");
            //当前交易金额与过去2次交易的标准差   标准差=（每个数-平均值）^2 的和   再除以个数        再开方
            double two_d = fangcha(two_list, cur_value);
            sb.append(two_d).append(",");
            //过去2次交易最大金额
            double two_e = Collections.max(two_list);
            sb.append(two_e).append(",");
        }
        //===================================================================
        //                          todo 前5次相关
        //===================================================================
        List<Integer> five_list = new ArrayList<>();
        try {
            five_list.add(list.get(size - 2));
        } catch (Exception e) {
            five_list.add(defaultValue);
        }
        try {
            five_list.add(list.get(size - 3));
        } catch (Exception e) {
            five_list.add(defaultValue);
        }
        try {
            five_list.add(list.get(size - 4));
        } catch (Exception e) {
            five_list.add(defaultValue);
        }
        try {
            five_list.add(list.get(size - 5));
        } catch (Exception e) {
            five_list.add(defaultValue);
        }
        try {
            five_list.add(list.get(size - 6));
        } catch (Exception e) {
            five_list.add(defaultValue);
        }
        if (five_list.contains(defaultValue)) {
            for (int i = 0; i < 5; i++) {
                sb.append(defaultValue).append(",");
            }
        } else {
            //过去5次平均交易金额
            double five_a = avgAmount(five_list);
            sb.append(five_a).append(",");
            //当前交易金额与过去5次交易金额均值之比
            double five_b = cur_value / five_a;
            sb.append(five_b).append(",");
            //当前交易金额与过去5次交易金额最大值之比
            double five_c = (double) cur_value / Collections.max(five_list);
            sb.append(five_c).append(",");
            //当前交易金额与过去5次交易的标准差
            double five_d = fangcha(five_list, cur_value);
            sb.append(five_d).append(",");
            //过去5次交易最大金额
            double five_e = Collections.max(five_list);
            sb.append(five_e).append(",");
        }
        //===================================================================
        //                          todo 前10次相关
        //===================================================================
        List<Integer> ten_list = new ArrayList<>();
        try {
            ten_list.add(list.get(size - 2));
        } catch (Exception e) {
            ten_list.add(defaultValue);
        }
        try {
            ten_list.add(list.get(size - 3));
        } catch (Exception e) {
            ten_list.add(defaultValue);
        }
        try {
            ten_list.add(list.get(size - 4));
        } catch (Exception e) {
            ten_list.add(defaultValue);
        }
        try {
            ten_list.add(list.get(size - 5));
        } catch (Exception e) {
            ten_list.add(defaultValue);
        }
        try {
            ten_list.add(list.get(size - 6));
        } catch (Exception e) {
            ten_list.add(defaultValue);
        }
        try {
            ten_list.add(list.get(size - 7));
        } catch (Exception e) {
            ten_list.add(defaultValue);
        }
        try {
            ten_list.add(list.get(size - 8));
        } catch (Exception e) {
            ten_list.add(defaultValue);
        }
        try {
            ten_list.add(list.get(size - 9));
        } catch (Exception e) {
            ten_list.add(defaultValue);
        }
        try {
            ten_list.add(list.get(size - 10));
        } catch (Exception e) {
            ten_list.add(defaultValue);
        }
        try {
            ten_list.add(list.get(size - 11));
        } catch (Exception e) {
            ten_list.add(defaultValue);
        }

        //如果包含默认值那么就认为10个相关的参数就无法计算
        if (ten_list.contains(defaultValue)) {
            for (int i = 0; i < 4; i++) {
                sb.append(defaultValue).append(",");
            }
            sb.append(defaultValue);
        } else {
            //过去10次平均交易金额
            double ten_a = avgAmount(ten_list);
            sb.append(ten_a).append(",");
            //当前交易金额与过去10次交易金额均值之比
            double ten_b = cur_value / ten_a;
            sb.append(ten_b).append(",");
            //当前交易金额与过去10次交易金额最大值之比
            double ten_c = (double) cur_value / Collections.max(ten_list);
            sb.append(ten_c).append(",");
            //当前交易金额与过去10次交易的标准差
            double ten_d = fangcha(ten_list, cur_value);
            sb.append(ten_d).append(",");
            //过去10次交易最大金额
            double ten_e = Collections.max(ten_list);
            sb.append(ten_e);
        }
        return sb.toString();
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
        if (queeSize == 11) {
            acc.queue.pollFirst();
        }
        acc.queue.add(value);
    }

}
