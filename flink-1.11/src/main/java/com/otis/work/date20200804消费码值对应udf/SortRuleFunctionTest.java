package com.otis.work.date20200804消费码值对应udf;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SortRuleFunctionTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        DataStreamSource<String> dataStream = env.socketTextStream("h101", 7777);

        //注册一个打印表
        String print_table = " CREATE TABLE print_table (" +
                "         id STRING," +
                "         type STRING" +
                "        ) WITH (" +
                "          'connector' = 'print'" +
                "        )";
        tableEnv.executeSql(print_table);

        DataStream<Data> map = dataStream.map(new MapFunction<String, Data>() {
            @Override
            public Data map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Data(arr[0], arr[1], arr[2], arr[3], arr[4], arr[5], arr[6], arr[7], arr[8]);
            }
        });

        //注册表
        tableEnv.createTemporaryView("source_table", map);
        //注册函数
        tableEnv.createFunction("myfunction", SortRuleFunction.class);
        tableEnv.executeSql("insert into print_table select '01',myfunction(accountName,account,currencyCode,transactionCode,transactionAmount,borrowFlage,businessCode,summary,accountChinessName) from source_table");

    }

    public static class Data {
        private String accountName;         //对方户名
        private String account;             //对方账号
        private String currencyCode;        //货币代码
        private String transactionCode;     //交易代码
        private String transactionAmount;      //交易金额
        private String borrowFlage;            //借贷标志
        private String businessCode;        //业务代号
        private String summary;             //摘要
        private String accountChinessName;  //账户中文名

        public String getAccountName() {
            return accountName;
        }

        public void setAccountName(String accountName) {
            this.accountName = accountName;
        }

        public String getAccount() {
            return account;
        }

        public void setAccount(String account) {
            this.account = account;
        }

        public String getCurrencyCode() {
            return currencyCode;
        }

        public void setCurrencyCode(String currencyCode) {
            this.currencyCode = currencyCode;
        }

        public String getTransactionCode() {
            return transactionCode;
        }

        public void setTransactionCode(String transactionCode) {
            this.transactionCode = transactionCode;
        }

        public String getTransactionAmount() {
            return transactionAmount;
        }

        public void setTransactionAmount(String transactionAmount) {
            this.transactionAmount = transactionAmount;
        }

        public String getBorrowFlage() {
            return borrowFlage;
        }

        public void setBorrowFlage(String borrowFlage) {
            this.borrowFlage = borrowFlage;
        }

        public String getBusinessCode() {
            return businessCode;
        }

        public void setBusinessCode(String businessCode) {
            this.businessCode = businessCode;
        }

        public String getSummary() {
            return summary;
        }

        public void setSummary(String summary) {
            this.summary = summary;
        }

        public String getAccountChinessName() {
            return accountChinessName;
        }

        public void setAccountChinessName(String accountChinessName) {
            this.accountChinessName = accountChinessName;
        }

        public Data() {
        }

        public Data(String accountName, String account, String currencyCode, String transactionCode, String transactionAmount, String borrowFlage, String businessCode, String summary, String accountChinessName) {
            this.accountName = accountName;
            this.account = account;
            this.currencyCode = currencyCode;
            this.transactionCode = transactionCode;
            this.transactionAmount = transactionAmount;
            this.borrowFlage = borrowFlage;
            this.businessCode = businessCode;
            this.summary = summary;
            this.accountChinessName = accountChinessName;
        }
    }
}
