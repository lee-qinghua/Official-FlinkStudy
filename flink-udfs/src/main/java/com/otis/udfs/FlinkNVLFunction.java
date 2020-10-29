package com.otis.udfs;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Date;


public class FlinkNVLFunction extends ScalarFunction {
    public String eval(String data1, String data2) {
        if (data1 == null) {
            return data2;
        } else {
            return data1;
        }
    }

    public Date eval(Date data1, Date data2) {
        if (data1 == null) {
            return data2;
        } else {
            return data1;
        }
    }

    public Float eval(Float data1, Float data2) {
        if (data1 == null) {
            return data2;
        } else {
            return data1;
        }
    }

    public Integer eval(Integer data1, Integer data2) {
        if (data1 == null) {
            return data2;
        } else {
            return data1;
        }
    }
}
