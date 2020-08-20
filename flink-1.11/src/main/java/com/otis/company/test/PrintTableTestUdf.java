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
                "'fields.rulecode.min'='1000'," +
                "'fields.rulecode.max'='9999'," +
                "'fields.amount.min'='100'," +
                "'fields.amount.max'='200'" +
                ")";

        String print_table = "" +
                "create TABLE print_table(" +
                "event_id int," +
                " rulecode int," +
                " amount int" +
                "        ) WITH (" +
                "          'connector' = 'print'" +
                "        )";
        String print_table_1 = "" +
                "create TABLE print_table_1(" +
                "event_id string," +
                " ttype string," +
                " amount int," +
                " ts TIMESTAMP(3)" +
                "        ) WITH (" +
                "          'connector' = 'print'" +
                "        )";
        String print_table_2 = "" +
                "create TABLE print_table_2(" +
                "ttype1 string," +
                " ccount1 BIGINT," +
                " amount1 int" +
                "        ) WITH (" +
                "          'connector' = 'print'" +
                "        )";

        /**
         * 对应的表为：
         * -- ts     transtype   field_1	  field_2
         * -- 001		贷款 		10			20
         * -- 001		消费		6			89
         * -- 001		转账		2			30
         */
        String print_table_3 = "" +
                "create TABLE print_table_3(" +
                "ttype1 string," +
                " ccount1 BIGINT," +
                " amount1 BIGINT," +
                " ts TIMESTAMP(0)" +
                "        ) WITH (" +
                "          'connector' = 'print'" +
                "        )";

        String print_table_4 = "" +
                "create TABLE print_table_4(" +
                " ccount1 BIGINT," +
                " amount1 BIGINT," +
                " ccount2 BIGINT," +
                " amount2 BIGINT" +
                "        ) WITH (" +
                "          'connector' = 'print'" +
                "        )";
        tableEnv.executeSql(dataGenStr);
        tableEnv.executeSql(print_table);
        tableEnv.executeSql(print_table_1);
        tableEnv.executeSql(print_table_2);
        tableEnv.executeSql(print_table_3);
        tableEnv.executeSql(print_table_4);
//        tableEnv.executeSql("insert into print_table select event_id,rulecode,amount from source_table");


//        tableEnv.executeSql("insert into print_table_1 select \n" +
//                "cast(event_id as string) eid,\n" +
//                "case\n" +
//                "when cast(rulecode as string) like '1%' then 'cunqian'\n" +
//                "when cast(rulecode as string) like '2%' then 'daikuan'\n" +
//                "when cast(rulecode as string) like '3%' then 'xiaofei'\n" +
//                "end as ttype,\n" +
//                "amount,\n" +
//                "ts from source_table");

        String sql = "insert into print_table_2 \n" +
                "        select\n" +
                "        ttype,\n" +
                "        count(eid) over w as ccount,\n" +
                "        sum(amount) over w as aamount\n" +
                "        from (\n" +
                "                select \n" +
                "                    cast(event_id as string) eid,\n" +
                "                    case\n" +
                "                    when cast(rulecode as string) like '1%' then 'xiaofei'\n" +
                "                    when cast(rulecode as string) like '2%' then 'bangong'\n" +
                "                    when cast(rulecode as string) like '3%' then 'xinzi'\n" +
                "                    when cast(rulecode as string) like '4%' then 'baoxian'\n" +
                "                    when cast(rulecode as string) like '5%' then 'touzi'\n" +
                "                    when cast(rulecode as string) like '6%' then 'rongzi'\n" +
                "                    when cast(rulecode as string) like '7%' then 'zhuanzhang'\n" +
                "                    when cast(rulecode as string) like '8%' then 'cunqu'\n" +
                "                    when cast(rulecode as string) like '9%' then 'other'\n" +
                "                    end as ttype,\n" +
                "                    amount,\n" +
                "                    ts from source_table\n" +
                "        )t1\n" +
                "        window w as (PARTITION BY ttype order by ts range between interval '20' second preceding and current row)";


        String a = "insert into print_table_2 select\n" +
                "    ttype,\n" +
                "    count(eid) over w as xiaofei_ccount,\n" +
                "    sum(amount) over w as xiaofei_aamount\n" +
                "    from (\n" +
                "            select\n" +
                "                cast(event_id as string) eid,\n" +
                "                case\n" +
                "                when cast(rulecode as string) like '1%' then 'xiaofei'\n" +
                "                when cast(rulecode as string) like '2%' then 'bangong'\n" +
                "                when cast(rulecode as string) like '3%' then 'xinzi'\n" +
                "                when cast(rulecode as string) like '4%' then 'baoxian'\n" +
                "                when cast(rulecode as string) like '5%' then 'touzi'\n" +
                "                when cast(rulecode as string) like '6%' then 'rongzi'\n" +
                "                when cast(rulecode as string) like '7%' then 'zhuanzhang'\n" +
                "                when cast(rulecode as string) like '8%' then 'cunqu'\n" +
                "                when cast(rulecode as string) like '9%' then 'other'\n" +
                "                end as ttype,\n" +
                "                amount,\n" +
                "                ts from source_table\n" +
                "    )t1 where ttype='xiaofei'\n" +
                "    window w as (order by ts range between interval '20' second preceding and current row)";

        String b = "select\n" +
                "    ttype,\n" +
                "    count(eid) over w as xiaofei_ccount,\n" +
                "    sum(amount) over w as xiaofei_aamount\n" +
                "    from (\n" +
                "            select\n" +
                "                cast(event_id as string) eid,\n" +
                "                case\n" +
                "                when cast(rulecode as string) like '1%' then 'xiaofei'\n" +
                "                when cast(rulecode as string) like '2%' then 'bangong'\n" +
                "                when cast(rulecode as string) like '3%' then 'xinzi'\n" +
                "                when cast(rulecode as string) like '4%' then 'baoxian'\n" +
                "                when cast(rulecode as string) like '5%' then 'touzi'\n" +
                "                when cast(rulecode as string) like '6%' then 'rongzi'\n" +
                "                when cast(rulecode as string) like '7%' then 'zhuanzhang'\n" +
                "                when cast(rulecode as string) like '8%' then 'cunqu'\n" +
                "                when cast(rulecode as string) like '9%' then 'other'\n" +
                "                end as ttype,\n" +
                "                amount,\n" +
                "                ts from source_table\n" +
                "    )t1 where ttype='xiaofei'\n" +
                "    window w as (order by ts range between interval '20' second preceding and current row)\n" +
                "union \n" +
                "select\n" +
                "    ttype,\n" +
                "    count(eid) over w as bangong_ccount,\n" +
                "    sum(amount) over w as bangong_aamount\n" +
                "    from (\n" +
                "            select\n" +
                "                cast(event_id as string) eid,\n" +
                "                case\n" +
                "                when cast(rulecode as string) like '1%' then 'xiaofei'\n" +
                "                when cast(rulecode as string) like '2%' then 'bangong'\n" +
                "                when cast(rulecode as string) like '3%' then 'xinzi'\n" +
                "                when cast(rulecode as string) like '4%' then 'baoxian'\n" +
                "                when cast(rulecode as string) like '5%' then 'touzi'\n" +
                "                when cast(rulecode as string) like '6%' then 'rongzi'\n" +
                "                when cast(rulecode as string) like '7%' then 'zhuanzhang'\n" +
                "                when cast(rulecode as string) like '8%' then 'cunqu'\n" +
                "                when cast(rulecode as string) like '9%' then 'other'\n" +
                "                end as ttype,\n" +
                "                amount,\n" +
                "                ts from source_table\n" +
                "    )t1 where ttype='bangong'\n" +
                "    window w as (order by ts range between interval '20' second preceding and current row)";

        String c = "insert into print_table_3 select\n" +
                "    ttype,\n" +
                "    count(eid) over w as field_01,\n" +
                "    sum(amount) over w as field_02,\n" +
                "     current_timestamp  as ts\n" +
                "    from (\n" +
                "            select\n" +
                "                cast(event_id as string) eid,\n" +
                "                case\n" +
                "                when cast(rulecode as string) like '1%' then 'xiaofei'\n" +
                "                when cast(rulecode as string) like '2%' then 'bangong'\n" +
                "                when cast(rulecode as string) like '3%' then 'xinzi'\n" +
                "                when cast(rulecode as string) like '4%' then 'baoxian'\n" +
                "                when cast(rulecode as string) like '5%' then 'touzi'\n" +
                "                when cast(rulecode as string) like '6%' then 'rongzi'\n" +
                "                when cast(rulecode as string) like '7%' then 'zhuanzhang'\n" +
                "                when cast(rulecode as string) like '8%' then 'cunqu'\n" +
                "                when cast(rulecode as string) like '9%' then 'other'\n" +
                "                end as ttype,\n" +
                "                amount,\n" +
                "                ts from source_table\n" +
                "    )t1\n" +
                "    window w as (partition by ttype order by ts range between interval '20' second preceding and current row)";


        String d = "select \n" +
                "    xiaofei_01,\n" +
                "    xiaofei_02,\n" +
                "    rongzi_01,\n" +
                "    rongzi_02\n" +
                "from (\n" +
                "        select\n" +
                "    'xiaofei',\n" +
                "    current_timestamp as  my_ts,\n" +
                "    count(eid) over w as xiaofei_01,\n" +
                "    sum(amount) over w as xiaofei_02\n" +
                "    from\n" +
                "    ( select\n" +
                "                cast(event_id as string) eid,\n" +
                "                case\n" +
                "                when cast(rulecode as string) like '1%' then 'xiaofei'\n" +
                "                when cast(rulecode as string) like '2%' then 'bangong'\n" +
                "                when cast(rulecode as string) like '3%' then 'xinzi'\n" +
                "                when cast(rulecode as string) like '4%' then 'baoxian'\n" +
                "                when cast(rulecode as string) like '5%' then 'touzi'\n" +
                "                when cast(rulecode as string) like '6%' then 'rongzi'\n" +
                "                when cast(rulecode as string) like '7%' then 'zhuanzhang'\n" +
                "                when cast(rulecode as string) like '8%' then 'cunqu'\n" +
                "                when cast(rulecode as string) like '9%' then 'other'\n" +
                "                end as ttype,\n" +
                "                amount,\n" +
                "                ts from source_table) a1 where ttype='xiaofei'\n" +
                "    window w as (order by ts range between interval '20' second preceding and current row)\n" +
                ")t1 join (\n" +
                "        select\n" +
                "    'rongzi',\n" +
                "     current_timestamp as my_ts,\n" +
                "    count(eid) over w as rongzi_01,\n" +
                "    sum(amount) over w as rongzi_02\n" +
                "    from\n" +
                "    (select\n" +
                "                cast(event_id as string) eid,\n" +
                "                case\n" +
                "                when cast(rulecode as string) like '1%' then 'xiaofei'\n" +
                "                when cast(rulecode as string) like '2%' then 'bangong'\n" +
                "                when cast(rulecode as string) like '3%' then 'xinzi'\n" +
                "                when cast(rulecode as string) like '4%' then 'baoxian'\n" +
                "                when cast(rulecode as string) like '5%' then 'touzi'\n" +
                "                when cast(rulecode as string) like '6%' then 'rongzi'\n" +
                "                when cast(rulecode as string) like '7%' then 'zhuanzhang'\n" +
                "                when cast(rulecode as string) like '8%' then 'cunqu'\n" +
                "                when cast(rulecode as string) like '9%' then 'other'\n" +
                "                end as ttype,\n" +
                "                amount,\n" +
                "                ts from source_table) a2 where ttype='rongzi'\n" +
                "    window w as (order by ts range between interval '20' second preceding and current row)\n" +
                ")t2 on t1.my_ts = t2.my_ts";
        tableEnv.executeSql(d);
    }
}
