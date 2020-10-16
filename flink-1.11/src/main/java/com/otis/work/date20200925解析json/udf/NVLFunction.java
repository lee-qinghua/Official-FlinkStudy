package com.otis.work.date20200925解析json.udf;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 如果传递的第一个值为null 就返回第二个值
 * 为什么不用if 因为if(boolean,value1,value2) value2没有我想要的类型
 */
public class NVLFunction extends ScalarFunction {
    public Object eval(DataTypes value1, DataTypes value2) {
        if (value1 == null) {
            return value2;
        } else {
            return value1;
        }
    }
}
