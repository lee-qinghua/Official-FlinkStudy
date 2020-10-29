package com.otis.udfs;

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
    //todo 这里可以使用方法的重载 根据传递参数类型的不同，调用不同的方法
//    Calendar d1 = Calendar.getInstance();
//    Calendar d2 = Calendar.getInstance();

//    public int eval(java.sql.Date date1, java.sql.Date date2) {
//        d1.setTime(date1);
//        d2.setTime(date2);
//        int diff1 = d1.get(Calendar.MONTH) - d2.get(Calendar.MONTH);
//        int diff2 = (d1.get(Calendar.YEAR) - d2.get(Calendar.YEAR)) * 12;
//        return diff1 + diff2;
//    }

    // 因为有的字段可能为null,但是必须这样计算的话 就返回9999
    // 这样计算后 如果还要进行比较，就需要加限制条件 !=9999
    public int eval(String date1, String date2) {
        if (date1 == null || date2 == null) {
            return 9999;
        }
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
