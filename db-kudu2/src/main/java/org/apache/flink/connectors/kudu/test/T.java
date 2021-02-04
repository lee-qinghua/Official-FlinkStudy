package org.apache.flink.connectors.kudu.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class T {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        String sql = "create table finicalinfo(\n" +
                "relation_id\tstring,\n" +
                "event_type\tstring,\n" +
                "user_id\tstring,\n" +
                "amt float,\n" +
                "event_dt\tbigint,\n" +
                "should_repay_time\tbigint,\n" +
                "repay_state\tstring,\n" +
                "actual_repay_time\tbigint\n" +
                ")with(\n" +
                "'connector'='kudu',\n" +
                "'kudu.table'='qinghua_finicalinfo'\n" +
                ")";
        tableEnv.executeSql(sql);
        tableEnv.executeSql("insert into finicalinfo values('005','0','user1',2.0,1612406576,1612407176,'0',0)");
    }
}
