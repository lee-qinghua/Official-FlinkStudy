package com.otis.方便测试的connector;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PrintTableTestUdf {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        DataStreamSource<String> dataStream = env.socketTextStream("h101", 7777);

        //生成数据
        String dataGenStr = " CREATE TABLE source_table (" +
                " event_id int," +
                " rulecode int," +
                " amount int," +
                " ts AS localtimestamp," +
                " WATERMARK FOR ts AS ts" +
                " ) WITH (" +
                "'connector' = 'datagen'," +
                "'rows-per-second'='5'," +
                "'fields.event_id.kind'='sequence'," +
                "'fields.event_id.start'='1'," +
                "'fields.event_id.end'='10000'," +
//                "'fields.rulecode.min'='1000'," +
//                "'fields.rulecode.max'='9999'," +
                "'fields.rulecode.min'='1'," +
                "'fields.rulecode.max'='9'," +
                "'fields.amount.min'='100'," +
                "'fields.amount.max'='200'" +
                ")";


//        String print_table = "" +
//                "create TABLE print_table(" +
//                "event_id int," +
//                " rulecode int," +
//                " amount int" +
//                "        ) WITH (" +
//                "          'connector' = 'print'" +
//                "        )";
//        String print_table_1 = "" +
//                "create TABLE print_table_1(" +
//                "event_id string," +
//                " ttype string," +
//                " amount int," +
//                " ts TIMESTAMP(3)" +
//                "        ) WITH (" +
//                "          'connector' = 'print'" +
//                "        )";
//        String print_table_2 = "" +
//                "create TABLE print_table_2(" +
//                "ttype1 string," +
//                " ccount1 BIGINT," +
//                " amount1 int" +
//                "        ) WITH (" +
//                "          'connector' = 'print'" +
//                "        )";
//
//        /**
//         * 对应的表为：
//         * -- ts     transtype   field_1	  field_2
//         * -- 001		贷款 		10			20
//         * -- 001		消费		6			89
//         * -- 001		转账		2			30
//         */
//        String print_table_3 = "" +
//                "create TABLE print_table_3(" +
//                "ttype1 string," +
//                " ccount1 BIGINT," +
//                " amount1 BIGINT," +
//                " ts TIMESTAMP(0)" +
//                "        ) WITH (" +
//                "          'connector' = 'print'" +
//                "        )";
//
//        String print_table_4 = "" +
//                "create TABLE print_table_4(" +
//                " ccount1 BIGINT," +
//                " amount1 BIGINT," +
//                " ccount2 BIGINT," +
//                " amount2 BIGINT" +
//                "        ) WITH (" +
//                "          'connector' = 'print'" +
//                "        )";
        String print_table_5 = "" +
                "create TABLE print_table_5(" +
                " ts TIMESTAMP(3)," +
                " xiaofei_amount int," +
                " bangong_amount int," +
                " xinzi_amount int," +
                " baoxian_amount int," +
                " touzi_amount int," +
                " rongzi_amount int," +
                " zhuanzhang_amount int," +
                " cunqu_amount int," +
                " other_amount int" +
                "        ) WITH (" +
                "          'connector' = 'print'" +
                "        )";
        String print_table_7 = "" +
                "create TABLE print_table_7(" +
                " ttype int," +
                " amount1 int," +
                " amount2 int" +
                "        ) WITH (" +
                "          'connector' = 'print'" +
                "        )";

        String print_table_6 = "" +
                "create TABLE print_table_6(" +
                " ttype int," +
                " amount1 int" +
                "        ) WITH (" +
                "          'connector' = 'print'" +
                "        )";

        String print_table_8 = "" +
                "create TABLE print_table_8(" +
                " rulecode int," +
                " amount1 int," +
                " amount2 boolean" +
                "        ) WITH (" +
                "          'connector' = 'print'" +
                "        )";
        tableEnv.executeSql(dataGenStr);
//        tableEnv.executeSql(print_table);
//        tableEnv.executeSql(print_table_1);
//        tableEnv.executeSql(print_table_2);
//        tableEnv.executeSql(print_table_3);
//        tableEnv.executeSql(print_table_4);
        tableEnv.executeSql(print_table_5);
        tableEnv.executeSql(print_table_7);
        tableEnv.executeSql(print_table_6);
        tableEnv.executeSql(print_table_8);

        String ca = "insert into print_table_8 select\n" +
                "t1.rulecode,\n" +
                "t1.amo as one_day_amount,\n" +
                "t2.amo>t1.amo as two_day_amount\n" +
                "from (\n" +
                "        select\n" +
                "            ts,\n" +
                "            rulecode,\n" +
                "            sum(amount) over (partition by rulecode order by ts range between interval '10' second preceding and current row) as amo\n" +
                "        from source_table\n" +
                ")t1,(\n" +
                "        select\n" +
                "            ts,\n" +
                "            rulecode,\n" +
                "            sum(amount) over (partition by rulecode order by ts range between interval '20' second preceding and current row)as amo\n" +
                "        from source_table\n" +
                ")t2\n" +
                "where t1.ts=t2.ts and t1.rulecode=t2.rulecode\n" +
                "      and t1.ts BETWEEN t2.ts - INTERVAL '1' second AND t2.ts + INTERVAL '1' second";
        tableEnv.executeSql(ca);
    }
}
