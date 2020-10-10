package com.otis.work.date20200925解析json.files;

import com.alibaba.fastjson.JSONArray;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Array;


/**
 * 作者：李清华
 * 功能：
 * 日期：2020/9/28-9:40
 */

public class Json2StringFunction extends ScalarFunction {

    public String eval(Array message) {
        String s = JSONArray.toJSONString(message);
        return s;
    }
}


