package com.otis.udfs;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

//@FunctionHint(
//        input = @DataTypeHint("ROW<f0 STRING, f1 STRING>")
//)


public class TestFunction extends ScalarFunction {
    public String eval(Row[] data) {

        for (Row row : data) {
            Row field = (Row) row.getField(1);
            String field1 = (String) row.getField(1);
            String field0 = (String) row.getField(0);
            return field0 + "----" + field1;
        }
        return "aaaaaa";
    }
}
