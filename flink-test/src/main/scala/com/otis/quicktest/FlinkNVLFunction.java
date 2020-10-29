package com.otis.quicktest;

import org.apache.flink.table.functions.ScalarFunction;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.Optional;


public class FlinkNVLFunction extends ScalarFunction {
    /**
     * 重载实现多个eval
     */
    public BigDecimal eval(BigDecimal value1, BigDecimal value2) {
        return Optional.ofNullable(value1).orElse(value2);
    }

    public BigDecimal eval(BigDecimal value1, Integer value2) {
        return Optional.ofNullable(value1).orElse(BigDecimal.valueOf(value2));
    }

    public Long eval(Long value1, Long value2) {
        return Optional.ofNullable(value1).orElse(value2);
    }

    public Long eval(Long value1, Integer value2) {
        return Optional.ofNullable(value1).orElse(value2.longValue());
    }

    public Integer eval(Integer value1, Integer value2) {
        return Optional.ofNullable(value1).orElse(value2);
    }
    public String eval(String value1, String value2) {
        return Optional.ofNullable(value1).orElse(value2);
    }
}
