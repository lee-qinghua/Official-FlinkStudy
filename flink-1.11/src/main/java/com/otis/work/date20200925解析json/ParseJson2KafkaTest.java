package com.otis.work.date20200925解析json;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 作者：李清华
 * 功能：解析json报文 生成表 发送到kafka
 * 日期：2020/9/25-13:48
 */

class ParseJson2KafkaTest {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String ods_table = "create table ods_table(\n" +
                "PRH ROW(PA01 ROW(\n" +
                "                  PA01B ROW(PA01BQ01 string,PA01BD01 string,PA01BI01 string,PA01BI02 string,PA01BD02 string),\n" +
                "                  PA01A ROW(PA01AI01 string,PA01AR01 string),\n" +
                "                  PA01C ROW(PA01CS01 string,PA01CH ARRAY<ROW(PA01CD01 string,PA01CI01 string)>),\n" +
                "                  PA01D ROW(PA01DQ01 string,PA01DQ02 string,PA01DR01 string,PA01DR02 string),\n" +
                "                  PA01E ROW(PA01ES01 STRING)\n" +
                "                 )),\n" +
                "PIM ROW(PB01 ROW(\n" +
                "                  PB01A ROW(PB01AD01 STRING,PB01AR01 STRING,PB01AD02 STRING,PB01AD03 STRING,PB01AD04 STRING,PB01AQ01 STRING,PB01AQ02 STRING,PB01AD05 STRING,PB01AQ03 STRING),\n" +
                "                  PB01B ROW(PB01BS01 STRING,PB01BH ARRAY<ROW(PB01BQ01 STRING,PB01BR01 STRING)>)\n" +
                "        )),\n" +
                "PMM ROW(PB02 ROW(PB020D01 STRING,PB020Q01 STRING,PB020D02 STRING,PB020I01 STRING,PB020Q02 STRING,PB020Q03 STRING)),\n" +
                "PRM ROW(PB03 ARRAY<ROW(PB030D01 STRING,PB030Q01 STRING,PB030Q02 STRING,PB030R01 STRING)>),\n" +
                "POM ROW(PB04 ARRAY<ROW(PB040D01 STRING,PB040Q01 STRING,PB040D02 STRING,PB040D03 STRING,PB040Q02 STRING,PB040Q03 STRING,PB040D04 STRING,PB040D05 STRING,PB040D06 STRING,PB040R01 STRING,PB040R02 STRING)>),\n" +
                "PSM ROW(PC01 ROW(PC010Q01 STRING,PC010Q02 STRING,PC010S01 STRING,PC010D01 STRING)),\n" +
                "PCO ROW(PC02 ROW(\n" +
                "                  PC02A ROW(PC02AS01 STRING,PC02AS02 STRING,PC02AH ARRAY<ROW(PC02AD01 STRING,PC02AD02 STRING,PC02AS03 STRING,PC02AR01 STRING)>),\n" +
                "                  PC02B ROW(PC02BS01 STRING,PC02BJ01 STRING,PC02BS02 STRING,PC02BH ARRAY<ROW(PC02BD01 STRING,PC02BS03 STRING,PC02BJ02 STRING)>),\n" +
                "                  PC02C ROW(PC02CS01 STRING,PC02CJ01 STRING),\n" +
                "                  PC02D ROW(PC02DS01 STRING,PC02DH ARRAY<ROW(PC02DD01 STRING,PC02DS02 STRING,PC02DS03 STRING,PC02DJ01 STRING,PC02DS04 STRING)>),\n" +
                "                  PC02E ROW(PC02ES01 STRING,PC02ES02 STRING,PC02EJ01 STRING,PC02EJ02 STRING,PC02EJ03 STRING),\n" +
                "                  PC02F ROW(PC02FS01 STRING,PC02FS02 STRING,PC02FJ01 STRING,PC02FJ02 STRING,PC02FJ03 STRING),\n" +
                "                  PC02G ROW(PC02GS01 STRING,PC02GS02 STRING,PC02GJ01 STRING,PC02GJ02 STRING,PC02GJ03 STRING),\n" +
                "                  PC02H ROW(PC02HS01 STRING,PC02HS02 STRING,PC02HJ01 STRING,PC02HJ02 STRING,PC02HJ03 STRING,PC02HJ04 STRING,PC02HJ05 STRING),\n" +
                "                  PC02I ROW(PC02IS01 STRING,PC02IS02 STRING,PC02IJ01 STRING,PC02IJ02 STRING,PC02IJ03 STRING,PC02IJ04 STRING,PC02IJ05 STRING),\n" +
                "                  PC02K ROW(PC02KS01 STRING,PC02KH ARRAY<ROW(PC02KD01 STRING,PC02KD02 STRING,PC02KS02 STRING,PC02KJ01 STRING,PC02KJ02 STRING)>))\n" +
                "      ),\n" +
                "PNO ROW(PC03 ROW(PC030S01 STRING,PC030H ARRAY<ROW(PC030D01 STRING,PC030S02 STRING,PC030J01 STRING)>)),\n" +
                "PPO ROW(PC04 ROW(PC040S01 STRING,PC040H ARRAY<ROW(PC040D01 STRING,PC040S02 STRING,PC040J01 STRING)>)),\n" +
                "PQO ROW(PC05 ROW(\n" +
                "                    PC05A ROW(PC05AR01 STRING,PC05AD01 STRING,PC05AI01 STRING,PC05AQ01 STRING),\n" +
                "                    PC05B ROW(PC05BS01 STRING,PC05BS02 STRING,PC05BS03 STRING,PC05BS04 STRING,PC05BS05 STRING,PC05BS06 STRING,PC05BS07 STRING,PC05BS08 STRING)\n" +
                "    )),\n" +
                "PDA ROW(PD01 ARRAY<ROW(\n" +
                "                    PD01A ROW(PD01AI01 STRING,PD01AD01 STRING,PD01AD02 STRING,PD01AI02 STRING,PD01AI03 STRING,PD01AI04 STRING,PD01AD03 STRING,PD01AR01 STRING,PD01AD04 STRING,PD01AJ01 STRING,PD01AJ02 STRING,PD01AJ03 STRING,PD01AR02 STRING,PD01AD05 STRING,PD01AD06 STRING,PD01AS01 STRING,PD01AD07 STRING,PD01AD08 STRING,PD01AD09 STRING,PD01AD10 STRING),\n" +
                "                    PD01B ROW(PD01BD01 STRING,PD01BR01 STRING,PD01BR04 STRING,PD01BJ01 STRING,PD01BR02 STRING,PD01BJ02 STRING,PD01BD03 STRING,PD01BD04 STRING,PD01BR03 STRING),\n" +
                "                    PD01C ROW(PD01CR01 STRING,PD01CD01 STRING,PD01CJ01 STRING,PD01CJ02 STRING,PD01CJ03 STRING,PD01CD02 STRING,PD01CS01 STRING,PD01CR02 STRING,PD01CJ04 STRING,PD01CJ05 STRING,PD01CR03 STRING,PD01CS02 STRING,PD01CJ06 STRING,PD01CJ07 STRING,PD01CJ08 STRING,PD01CJ09 STRING,PD01CJ10 STRING,PD01CJ11 STRING,PD01CJ12 STRING,PD01CJ13 STRING,PD01CJ14 STRING,PD01CJ15 STRING,PD01CR04 STRING),\n" +
                "                    PD01D ROW(PD01DR01 STRING,PD01DR02 STRING,PD01DH ARRAY<ROW(PD01DR03 STRING,PD01DD01 STRING)>),\n" +
                "                    PD01E ROW(PD01ER01 STRING,PD01ER02 STRING,PD01ES01 STRING,PD01EH ARRAY<ROW(PD01ER03 STRING,PD01ED01 STRING,PD01EJ01 STRING)>),\n" +
                "                    PD01F ROW(PD01FS01 STRING,PD01FH ARRAY<ROW(PD01FD01 STRING,PD01FR01 STRING,PD01FS02 STRING,PD01FJ01 STRING,PD01FQ01 STRING)>),\n" +
                "                    PD01G ROW(PD01GS01 STRING,PD01GH ARRAY<ROW(PD01GR01 STRING,PD01GD01 STRING)>),\n" +
                "                    PD01H ROW(PD01HS01 STRING,PD01HH ARRAY<ROW(PD01HJ01 STRING,PD01HR01 STRING,PD01HR02 STRING,PD01HJ02 STRING)>),\n" +
                "                    PD01Z ROW(PD01ZS01 STRING,PD01ZH ARRAY<ROW(PD01ZD01 STRING,PD01ZQ01 STRING,PD01ZR01 STRING)>)\n" +
                "      )>),\n" +
                "PCA ROW(PD02 ARRAY<ROW(\n" +
                "                        PD02A ROW(PD02AI01 STRING,PD02AD01 STRING,PD02AI02 STRING,PD02AI03 STRING,PD02AD02 STRING,PD02AJ01 STRING,PD02AD03 STRING,PD02AR01 STRING,PD02AR02 STRING,PD02AD04 STRING,PD02AJ04 STRING,PD02AJ03 STRING,PD02AI04 STRING),\n" +
                "                        PD02Z ROW(PD02ZS01 STRING,PD02ZH ARRAY<ROW(PD02ZD01 STRING,PD02ZQ01 STRING,PD02ZR01 STRING)>)\n" +
                "                        )>),\n" +
                "PCR ROW(PD03 ARRAY<ROW(\n" +
                "                        PD03A ROW(PD03AD08 STRING,PD03AD01 STRING,PD03AQ01 STRING,PD03AD02 STRING,PD03AR01 STRING,PD03AR02 STRING,PD03AD03 STRING,PD03AQ02 STRING,PD03AJ01 STRING,PD03AD04 STRING,PD03AJ02 STRING,PD03AD05 STRING,PD03AD06 STRING,PD03AD07 STRING,PD03AS01 STRING,PD03AR03 STRING),\n" +
                "                        PD03Z ROW(PD03ZS01 STRING,PD03ZH ARRAY<ROW(PD03ZD01 STRING)>)\n" +
                "                      )>),\n" +
                "PND ROW(PE01 ARRAY<ROW(\n" +
                "                        PE01A ROW(PE01AD01 STRING,PE01AQ01 STRING,PE01AD02 STRING,PE01AR01 STRING,PE01AD03 STRING,PE01AJ01 STRING,PE01AR02 STRING,PE01AQ02 STRING),\n" +
                "                        PE01Z ROW(PE01ZS01 STRING,PE01ZH ARRAY<ROW(PE01ZD01 STRING,PE01ZQ01 STRING,PE01ZR01 STRING)>)\n" +
                "                )>),\n" +
                "POT ROW(PF01 ARRAY<ROW(\n" +
                "                        PF01A ROW(PF01AQ01 STRING,PF01AJ01 STRING,PF01AR01 STRING),\n" +
                "                        PF01Z ROW(PF01ZS01 STRING,PF01ZH ARRAY<ROW(PF01ZD01 STRING,PF01ZQ01 STRING,PF01ZR01 STRING)>))\n" +
                "                >),\n" +
                "PCJ ROW(PF02 ARRAY<ROW(\n" +
                "                        PF02A ROW(PF02AQ01 STRING,PF02AQ02 STRING,PF02AR01 STRING,PF02AD01 STRING,PF02AQ03 STRING,PF02AR02 STRING,PF02AQ04 STRING,PF02AJ01 STRING),\n" +
                "                        PF02Z ROW(PF02ZS01 STRING,PF02ZH ARRAY<ROW(PF02ZD01 STRING,PF02ZQ01 STRING,PF02ZR01 STRING)>)\n" +
                "                )>),\n" +
                "PCE ROW(PF03 ARRAY<ROW(\n" +
                "                        PF03A ROW(PF03AQ01 STRING,PF03AQ02 STRING,PF03AR01 STRING,PF03AD01 STRING,PF03AQ03 STRING,PF03AR02 STRING,PF03AQ04 STRING,PF03AJ01 STRING,PF03AQ05 STRING,PF03AJ02 STRING),\n" +
                "                        PF03Z ROW(PF03ZS01 STRING,PF03ZH ARRAY<ROW(PF03ZD01 STRING,PF03ZQ01 STRING,PF03ZR01 STRING)>)\n" +
                "                )>),\n" +
                "PAP ROW(PF04 ARRAY<ROW(\n" +
                "                        PF04A ROW(PF04AQ01 STRING,PF04AQ02 STRING,PF04AJ01 STRING,PF04AR01 STRING,PF04AR02 STRING,PF04AQ03 STRING),\n" +
                "                        PF04Z ROW(PF04ZS01 STRING,PF04ZH ARRAY<ROW(PF04ZD01 STRING,PF04ZQ01 STRING,PF04ZR01 STRING)>)\n" +
                "                )>),\n" +
                "PHF ROW(PF05 ARRAY<ROW(\n" +
                "                        PF05A ROW(PF05AQ01 STRING,PF05AR01 STRING,PF05AD01 STRING,PF05AR02 STRING,PF05AR03 STRING,PF05AQ02 STRING,PF05AQ03 STRING,PF05AJ01 STRING,PF05AQ04 STRING,PF05AR04 STRING),\n" +
                "                        PF05Z ROW(PF05ZS01 STRING,PF05ZH ARRAY<ROW(PF05ZD01 STRING,PF05ZQ01 STRING,PF05ZR01 STRING)>)\n" +
                "                )>),\n" +
                "PBS ROW(PF06 ARRAY<ROW(\n" +
                "                        PF06A ROW(PF06AD01 STRING,PF06AQ01 STRING,PF06AQ02 STRING,PF06AQ03 STRING,PF06AR01 STRING,PF06AR02 STRING,PF06AR03 STRING),\n" +
                "                        PF06Z ROW(PF06ZS01 STRING,PF06ZH ARRAY<ROW(PF06ZD01 STRING,PF06ZQ01 STRING,PF06ZR01 STRING)>)\n" +
                "                )>),\n" +
                "PPQ ROW(PF07 ARRAY<ROW(\n" +
                "                        PF07A ROW(PF07AQ01 STRING,PF07AQ02 STRING,PF07AD01 STRING,PF07AD02 STRING,PF07AR01 STRING,PF07AR02 STRING,PF07AR03 STRING),\n" +
                "                        PF07Z ROW(PF07ZS01 STRING,PF07ZH ARRAY<ROW(PF07ZD01 STRING,PF07ZQ01 STRING,PF07ZR01 STRING)>)\n" +
                "                )>),\n" +
                "PAH ROW(PF08 ARRAY<ROW(\n" +
                "                        PF08A ROW(PF08AQ01 STRING,PF08AQ02 STRING,PF08AR01 STRING,PF08AR02 STRING),\n" +
                "                        PF08Z ROW(PF08ZS01 STRING,PF08ZH ARRAY<ROW(PF08ZD01 STRING,PF08ZQ01 STRING,PF08ZR01 STRING)>)\n" +
                "                )>),\n" +
                "POS ROW(PG01 ARRAY<ROW(PG010D01 STRING,PG010D02 STRING)>),\n" +
                "POQ ROW(PH01 ARRAY<ROW(PH010R01 STRING,PH010D01 STRING,PH010Q02 STRING,PH010Q03 STRING)>)\n" +
                ")WITH(\n" +
                "'connector' = 'kafka'," +
                "'topic' = 'odsTable'," +
                "'properties.bootstrap.servers' = '10.1.30.8:9092'," +
                "'properties.group.id' = 'topic.group1'," +
                "'format' = 'json'," +
                "'scan.startup.mode' = 'earliest-offset'" +
                ")\n";
        tableEnv.executeSql(ods_table);
        //===========================================================================================================================================
        //                                                          todo ICR_SCORE_DESC
        //===========================================================================================================================================
        String ICR_SCORE_DESC = " select\n" +
                "                  PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                  PSM.PC01.PC010D01                       as score_cd,\n" +
                "                  PRH.PA01.PA01A.PA01AI01                 as SID,\n" +
                "                  '2020-09-27'                            as STATISTICS_DT\n" +
                "                  from ods_table";


        createView(tableEnv, ICR_SCORE_DESC, "ICR_SCORE_DESC");
        //===========================================================================================================================================
        //                                                          todo ICR_CREDIT_CUE_NUM
        //===========================================================================================================================================
        String ICR_CREDIT_CUE_NUM = " select\n" +
                "        PRH.PA01.PA01A.PA01AI01 as report_id,\n" +
                "                PCO.PC02.PC02A.PC02AS01 as acct_total_cnt,\n" +
                "        PCO.PC02.PC02A.PC02AS02 as busi_type_num,\n" +
                "                '2020-09-27' as STATISTICS_DT\n" +
                "        from ods_table";


        createView(tableEnv, ICR_CREDIT_CUE_NUM, "ICR_CREDIT_CUE_NUM");

        String sink = "CREATE TABLE sink_table3 (\n" +
                "    a1 string,\n" +
                "    a2 string,\n" +
                "    a3 string,\n" +
                "    a4 string\n" +
                "    )\n" +
                "    WITH (\n" +
                "      'connector' = 'kafka',\n" +
                "      'topic' = 'qinghuatest-011',\n" +
                "      'properties.group.id'='dev_flink',\n" +
                "      'properties.zookeeper.connect'='10.1.30.6:2181',\n" +
                "      'properties.bootstrap.servers' = '10.1.30.8:9092',\n" +
                "      'format' = 'json'\n" +
                "      )";
        tableEnv.executeSql(sink);
        tableEnv.executeSql("insert into sink_table3 select * from ICR_CREDIT_CUE_NUM");
        tableEnv.execute("haha");
    }

    /**
     * 创建view的方法
     *
     * @param tableEnv
     * @param sql
     * @param tableName
     */
    public static void createView(StreamTableEnvironment tableEnv, String sql, String tableName) {
        Table table = tableEnv.sqlQuery(sql);
        tableEnv.createTemporaryView(tableName, table);
    }
}