package com.otis.work.date20200925解析json.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 功能和hive 里边的 months_between相似，date1>date2 不然会出现负数
 * 测试成功 ok
 */
public class MonthsBetweenStr extends ScalarFunction {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    public int eval(String date1, String date2) {
        Date date11 = null;
        Date date22 = null;
        try {
            date11 = format.parse(date1);
            date22 = format.parse(date2);
            Calendar d1 = Calendar.getInstance();
            Calendar d2 = Calendar.getInstance();

            d1.setTime(date11);
            d2.setTime(date22);
            int diff1 = d1.get(Calendar.MONTH) - d2.get(Calendar.MONTH);
            int diff2 = (d1.get(Calendar.YEAR) - d2.get(Calendar.YEAR)) * 12;
            return diff1 + diff2;
        } catch (ParseException e) {
            System.out.println(e.toString());
        }
        return -999;
    }
}
