package com.otis.work.date20200925解析json.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class test {
    public static void main(String[] args) {

            SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
            try {
                Date date1 = sf.parse("2020-04-29");
                Date date2 = sf.parse("2019-03-02");
                Calendar d1 = Calendar.getInstance();
                Calendar d2 = Calendar.getInstance();
                d1.setTime(date1);
                d2.setTime(date2);
                int diff1 = d1.get(Calendar.MONTH) - d2.get(Calendar.MONTH);
                int diff2 = (d1.get(Calendar.YEAR) - d2.get(Calendar.YEAR)) * 12;
                System.out.println(diff1+diff2);
            } catch (ParseException e) {
                e.printStackTrace();
            }



    }
}
