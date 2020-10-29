package com.otis.udfs

import org.apache.flink.table.data.RowData
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.DataTypes._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

class TTFunction(a:ROW(myField INT, myOtherField BOOLEAN)) extends ScalarFunction {

}
