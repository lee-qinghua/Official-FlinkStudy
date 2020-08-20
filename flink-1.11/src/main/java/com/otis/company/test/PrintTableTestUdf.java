package com.otis.company.test;

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
        tableEnv.executeSql(dataGenStr);
//        tableEnv.executeSql(print_table);
//        tableEnv.executeSql(print_table_1);
//        tableEnv.executeSql(print_table_2);
//        tableEnv.executeSql(print_table_3);
//        tableEnv.executeSql(print_table_4);
        tableEnv.executeSql(print_table_5);
        tableEnv.executeSql(print_table_7);
        tableEnv.executeSql(print_table_6);

        String ab="insert into print_table_5 select\n" +
                "    ts,\n" +
                "    if(ttype='xiaofei',amount,0) xiaofei_amount,\n" +
                "    if(ttype='bangong',amount,0) bangong_amount,\n" +
                "    if(ttype='xinzi',amount,0) xinzi_amount,\n" +
                "    if(ttype='baoxian',amount,0) baoxian_amount,\n" +
                "    if(ttype='touzi',amount,0) touzi_amount,\n" +
                "    if(ttype='rongzi',amount,0) rongzi_amount,\n" +
                "    if(ttype='zhuanzhang',amount,0) zhuanzhang_amount,\n" +
                "    if(ttype='cunqu',amount,0) cunqu_amount,\n" +
                "    if(ttype='other',amount,0) other_amount\n" +
                "from (" +
                    " select\n" +
                    "       case\n" +
                    "       when cast(rulecode as string) like '1%' then 'xiaofei'\n" +
                    "       when cast(rulecode as string) like '2%' then 'bangong'\n" +
                    "       when cast(rulecode as string) like '3%' then 'xinzi'\n" +
                    "       when cast(rulecode as string) like '4%' then 'baoxian'\n" +
                    "       when cast(rulecode as string) like '5%' then 'touzi'\n" +
                    "       when cast(rulecode as string) like '6%' then 'rongzi'\n" +
                    "       when cast(rulecode as string) like '7%' then 'zhuanzhang'\n" +
                    "       when cast(rulecode as string) like '8%' then 'cunqu'\n" +
                    "       when cast(rulecode as string) like '9%' then 'other'\n" +
                    "       end as ttype,\n" +
                    "       amount,\n" +
                    "       ts \n" +
                "from source_table)t1";

        String aa="insert into print_table_7 " +
                "select\n" +
                "rulecode,\n" +
                "sum(amount) over (PARTITION BY rulecode order by ts RANGE BETWEEN INTERVAL '10' second preceding and current row) amo1 ,\n" +
                "sum(amount) over (partition by rulecode order by ts RANGE between INTERVAL '20' second preceding and current row) \n" +
                "from source_table" ;
//                "window w1 as (PARTITION BY rulecode order by ts RANGE BETWEEN INTERVAL '10' second preceding and current row)\n" +
//                "window w2 as (partition by rulecode order by ts RANGE between INTERVAL '20' second preceding and current row)";

        String bb="insert into print_table_6 " +
                "select\n" +
                "rulecode,\n" +
                "sum(amount) over w1 amo1 \n" +
                "from source_table\n" +
                "window w1 as (PARTITION BY rulecode order by ts RANGE BETWEEN INTERVAL '10' second preceding and current row)" ;

        tableEnv.executeSql(aa);
    }
}
