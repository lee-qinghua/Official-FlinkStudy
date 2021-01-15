package com.otis.udfs;


import org.apache.flink.table.functions.AggregateFunction;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ContinuousCountFunction extends AggregateFunction<Integer, AccValue> {

    @Override
    public Integer getValue(AccValue acc) {
        // 现在同意报告编号下的，卡号对应的还款信息都在accumulator中了，判断有连续target个月连续还款的账户个数
        int target = acc.target;
        Map<String, List<Pojo1>> map = acc.getMap();

        int count = 0;
        for (List<Pojo1> list : map.values()) {
            int num = maxNum(list);
            if (num >= target) count++;
        }
        return count;
    }

    //求最大连续还款次数
    public int maxNum(List<Pojo1> list) {
        //先对数据按照月份从小到大进行排序
        list.sort(new Comparator<Pojo1>() {
            @Override
            public int compare(Pojo1 o1, Pojo1 o2) {
                int t1 = Integer.parseInt(o1.ttime.replace("-", ""));
                int t2 = Integer.parseInt(o2.ttime.replace("-", ""));
                return t1 - t2;
            }
        });
        // 再加过滤条件 and repay_stat_24m in ('1','2','3','4','5','6','7')
        String[] arr = {"1", "2", "3", "4", "5", "6", "7"};
        List<String> filter = Arrays.asList(arr);
        //找出连续最大次数
        int max = 0;
        int mid = 0;
        Calendar last = Calendar.getInstance();
        last.setTime(new Date(0));

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
        Calendar cur = Calendar.getInstance();

        try {
            for (Pojo1 pojo1 : list) {
                if (!filter.contains(pojo1.status)) continue;
                Date date = format.parse(pojo1.ttime);
                cur.setTime(date);
                last.add(Calendar.MONTH, 1);
                if (last.getTime().getTime() == cur.getTime().getTime()) {
                    //两个时间是连续的月
                    mid += 1;
                    max = Math.max(max, mid);
                } else {
                    //两个时间不连续
                    last.setTime(date);
                    mid = 1;
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return max;
    }

    @Override
    public AccValue createAccumulator() {
        AccValue accValue = new AccValue();
        accValue.setMap(new HashMap<String, List<Pojo1>>());
        return accValue;
    }

    /**
     * @param acc     累加器
     * @param cardNum 卡号
     * @param time    还款时间
     * @param status  还款状态
     * @param target  判断连续还款的月数
     *                此方法每进来一条数据计算一次
     */
    public void accumulate(AccValue acc, String cardNum, String time, String status, int target) {
        Pojo1 pojo1 = new Pojo1(time, status);

        Map<String, List<Pojo1>> oldmap = acc.getMap();
        List<Pojo1> oldlist = oldmap.get(cardNum);

        if (oldlist == null) oldlist = new ArrayList<Pojo1>();
        oldlist.add(pojo1);

        oldmap.put(cardNum, oldlist);
        acc.setMap(oldmap);
        acc.setTarget(target);
    }
}
