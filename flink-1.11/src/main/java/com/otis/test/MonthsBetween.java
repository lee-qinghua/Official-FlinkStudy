package com.otis.test;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Date;
import java.util.Calendar;

/**
 * 功能和hive 里边的 months_between相似，date1>date2 不然会出现负数
 */
public class MonthsBetween extends ScalarFunction {

    public int eval(Date date1, Date date2) {
        Calendar d1 = Calendar.getInstance();
        Calendar d2 = Calendar.getInstance();

        d1.setTime(date1);
        d2.setTime(date2);
        int diff1 = d1.get(Calendar.MONTH) - d2.get(Calendar.MONTH);
        int diff2 = (d1.get(Calendar.YEAR) - d2.get(Calendar.YEAR)) * 12;
        return diff1 + diff2;
    }
}
