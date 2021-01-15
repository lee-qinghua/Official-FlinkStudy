package com.otis.udfs;

import org.apache.flink.table.functions.ScalarFunction;

public class Demo1 extends ScalarFunction {
    public String eval(String s) {
        return s + "================";
    }
}
