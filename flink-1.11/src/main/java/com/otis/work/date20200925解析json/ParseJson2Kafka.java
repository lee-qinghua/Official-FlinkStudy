package com.otis.work.date20200925解析json;

import com.otis.work.date20200925解析json.udf.MonthsBetweenStr;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.utils.PrintUtils;
import org.apache.flink.types.Row;

import java.io.PrintWriter;

/**
 * 作者：李清华
 * 功能：解析json报文 生成表 发送到kafka
 * 日期：2020/9/25-13:48
 */

public class ParseJson2Kafka {
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
                "                        PD03Z ROW(PD03ZS01 STRING,PD03ZH ARRAY<ROW(PD03ZD01 STRING,PD03ZQ01 STRING,PD03ZR01 STRING)>)\n" +
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
                "POS ROW(PG01 ARRAY<ROW(PG010D01 STRING,PG010D02 STRING,PG010S01 STRING,PG010H ARRAY<ROW(PG010D03 STRING,PG010Q01 STRING,PG010R01 STRING)>)>),\n" +
                "POQ ROW(PH01 ARRAY<ROW(PH010R01 STRING,PH010D01 STRING,PH010Q02 STRING,PH010Q03 STRING)>)\n" +
                ")WITH(\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'odsTable',\n" +
                "'properties.bootstrap.servers' = '10.1.30.8:9092',\n" +
                "'properties.group.id' = 'topic.group1',\n" +
                "'format' = 'json',\n" +
                "'scan.startup.mode' = 'earliest-offset'\n" +
                ")";

        tableEnv.executeSql(ods_table);
        //===========================================================================================================================================
        //                                                          todo ICR_QUERYREQ
        //===========================================================================================================================================
        tableEnv.createFunction("flink_months_between_str", MonthsBetweenStr.class);
        String ICR_QUERYREQ = "select\n" +
                "PRH.PA01.PA01A.PA01AI01 as report_id,\n" +
                "PRH.PA01.PA01A.PA01AI01 as report_no,\n" +
                "PRH.PA01.PA01A.PA01AR01 as report_tm,\n" +
                "PRH.PA01.PA01B.PA01BQ01 as cust_name,\n" +
                "PRH.PA01.PA01B.PA01BD01 as query_iden_cd,\n" +
                "PRH.PA01.PA01B.PA01BI01 as query_iden_id,\n" +
                "PRH.PA01.PA01B.PA01BI02 as query_org_id,\n" +
                "PRH.PA01.PA01B.PA01BD02 as query_reason_cd,\n" +
                "CURRENT_DATE            as STATISTICS_DT\n" +
                "from ods_table";
        createView(tableEnv, ICR_QUERYREQ, "ICR_QUERYREQ");
        //===========================================================================================================================================
        //                                                          todo ICR_OTHER_IDEN_NUM
        //===========================================================================================================================================
        String ICR_OTHER_IDEN_NUM_table = "select\n" +
                "PRH.PA01.PA01A.PA01AI01 as report_id,\n" +
                "PRH.PA01.PA01C.PA01CS01 as iden_type_num,\n" +
                "CURRENT_DATE            as STATISTICS_DT\n" +
                "from ods_table";
        createView(tableEnv, ICR_OTHER_IDEN_NUM_table, "ICR_OTHER_IDEN_NUM_table");
        //===========================================================================================================================================
        //                                                          todo ICR_OTHER_IDEN
        //===========================================================================================================================================

        String ICR_OTHER_IDEN = "select\n" +
                "                report_id as report_id,\n" +
                "                t2.a      as iden_cd,\n" +
                "                t2.b      as iden_id,\n" +
                "                SID\t\t  as SID,\n" +
                "                STATISTICS_DT as STATISTICS_DT\n" +
                "                from\n" +
                "                        (select\n" +
                "                                PRH.PA01.PA01A.PA01AI01 as SID,\n" +
                "                                PRH.PA01.PA01A.PA01AI01 as report_id,\n" +
                "                                PRH.PA01.PA01C.PA01CH as ok,\n" +
                "                                CURRENT_DATE            as STATISTICS_DT\n" +
                "                                from ods_table)t1,\n" +
                "                unnest(t1.ok) as t2(a,b) where t1.ok is not null";
        createView(tableEnv, ICR_OTHER_IDEN, "ICR_OTHER_IDEN");
        //===========================================================================================================================================
        //                                                          todo ICR_FRAUD
        //===========================================================================================================================================

        String ICR_FRAUD = " select\n" +
                " PRH.PA01.PA01A.PA01AI01 as report_id,\n" +
                " PRH.PA01.PA01D.PA01DQ01 as fraud_cd,\n" +
                " PRH.PA01.PA01D.PA01DQ02 as fraud_tel,\n" +
                " PRH.PA01.PA01D.PA01DR01 as fraud_start_dt,\n" +
                " PRH.PA01.PA01D.PA01DR02 as fraud_end_dt,\n" +
                " cast(PRH.PA01.PA01E.PA01ES01 as bigint) as objection_num,\n" +
                " CURRENT_DATE            as STATISTICS_DT\n" +
                " from ods_table";

        createView(tableEnv, ICR_FRAUD, "ICR_FRAUD");

        //===========================================================================================================================================
        //                                                          todo ICR_IDENTITY
        //===========================================================================================================================================

        String ICR_IDENTITY = "select\n" +
                "PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "PIM.PB01.PB01A.PB01AD01                 as gender_cd,\n" +
                "PIM.PB01.PB01A.PB01AR01                 as birth_date,\n" +
                "PIM.PB01.PB01A.PB01AD02                 as edu_level_cd,\n" +
                "PIM.PB01.PB01A.PB01AD03                 as edu_degree_cd,\n" +
                "PIM.PB01.PB01A.PB01AD04                 as employment_cd,\n" +
                "PIM.PB01.PB01A.PB01AQ01                 as email,\n" +
                "PIM.PB01.PB01A.PB01AQ02                 as comm_addr,\n" +
                "PIM.PB01.PB01A.PB01AD05                 as nationality,\n" +
                "PIM.PB01.PB01A.PB01AQ03                 as reg_addr,\n" +
                "cast(PIM.PB01.PB01B.PB01BS01 as bigint) as tel_cnt,\n" +
                "CURRENT_DATE                            as STATISTICS_DT\n" +
                "from ods_table";
        createView(tableEnv, ICR_IDENTITY, "ICR_IDENTITY");

        //===========================================================================================================================================
        //                                                          todo ICR_TEL
        //===========================================================================================================================================
        String ICR_TEL = "select\n" +
                "t1.report_id                            as report_id,\n" +
                "info.PB01BQ01                           as tel_num,\n" +
                "info.PB01BR01                           as update_dt,\n" +
                "t1.SID                                  as SID,\n" +
                "t1.STATISTICS_DT                        as STATISTICS_DT\n" +
                "from(\n" +
                "        SELECT\n" +
                "        PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "        PIM.PB01.PB01B.PB01BH                   as data,\n" +
                "        PRH.PA01.PA01A.PA01AI01                 as SID,\n" +
                "        CURRENT_DATE                            as STATISTICS_DT\n" +
                "        FROM ods_table)t1,unnest(t1.data) as info(PB01BQ01,PB01BR01) where t1.data is not null";
        createView(tableEnv, ICR_TEL, "ICR_TEL");
        //===========================================================================================================================================
        //                                                          todo ICR_SPOUSE
        //===========================================================================================================================================
        String ICR_SPOUSE = "SELECT\n" +
                "PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "PMM.PB02.PB020D01                       as marital_stat_cd,\n" +
                "PMM.PB02.PB020Q01                       as spo_name,\n" +
                "PMM.PB02.PB020D02                       as spo_iden_cd,\n" +
                "PMM.PB02.PB020I01                       as spo_iden_id,\n" +
                "PMM.PB02.PB020Q02                       as spo_unit,\n" +
                "PMM.PB02.PB020Q03                       as spo_tel_num,\n" +
                "CURRENT_DATE                            as STATISTICS_DT\n" +
                "FROM ods_table";
        createView(tableEnv, ICR_SPOUSE, "ICR_SPOUSE");
        //===========================================================================================================================================
        //                                                          todo ICR_RESIDENCE
        //===========================================================================================================================================

        String ICR_RESIDENCE = "select\n" +
                "t1.report_id                            as report_id,\n" +
                "info.PB030D01                           as res_cd,\n" +
                "info.PB030Q01                           as res_addr,\n" +
                "info.PB030Q02                           as res_tel,\n" +
                "info.PB030R01                           as res_update_dt,\n" +
                "t1.SID                                  as SID,\n" +
                "t1.STATISTICS_DT                        as STATISTICS_DT\n" +
                "from\n" +
                "        (\n" +
                "                select\n" +
                "                PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                PRM.PB03                                as data,\n" +
                "                PRH.PA01.PA01A.PA01AI01                 as SID,\n" +
                "                CURRENT_DATE                            as STATISTICS_DT\n" +
                "                from ods_table\n" +
                "        )t1,unnest(t1.data) as info(PB030D01,PB030Q01,PB030Q02,PB030R01) where t1.data is not null";
        createView(tableEnv, ICR_RESIDENCE, "ICR_RESIDENCE");
        //===========================================================================================================================================
        //                                                          todo ICR_PROFESSION
        //===========================================================================================================================================
        String ICR_PROFESSION = "select\n" +
                "t1.report_id                            as report_id,\n" +
                "info.PB040D01                           as work_situation,\n" +
                "info.PB040Q01                           as work_unit,\n" +
                "info.PB040D02                           as unit_property_cd,\n" +
                "info.PB040D03                           as industry_cd,\n" +
                "info.PB040Q02                           as unit_addr,\n" +
                "info.PB040Q03                           as unit_tel,\n" +
                "info.PB040D04                           as occupation_cd,\n" +
                "info.PB040D05                           as position_cd,\n" +
                "info.PB040D06                           as title_cd,\n" +
                "info.PB040R01                           as int_year,\n" +
                "info.PB040R02                           as pro_update_date,\n" +
                "t1.SID                                  as SID,\n" +
                "t1.STATISTICS_DT                        as STATISTICS_DT\n" +
                "from\n" +
                "        (\n" +
                "                select\n" +
                "                PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                POM.PB04                                as data,\n" +
                "                PRH.PA01.PA01A.PA01AI01                 as SID,\n" +
                "                CURRENT_DATE                            as STATISTICS_DT\n" +
                "                from ods_table\n" +
                "        )t1,unnest(t1.data) as info(PB040D01,PB040Q01,PB040D02,PB040D03,PB040Q02,PB040Q03,PB040D04,PB040D05,PB040D06,PB040R01,PB040R02) where t1.data is not null";

        createView(tableEnv, ICR_PROFESSION, "ICR_PROFESSION");
        //===========================================================================================================================================
        //                                                          todo ICR_CREDITSCORE
        //===========================================================================================================================================
        String ICR_CREDITSCORE = "select\n" +
                "PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "PSM.PC01.PC010Q01                       as score,\n" +
                "PSM.PC01.PC010Q02                       as score_level,\n" +
                "PSM.PC01.PC010S01                       as score_desc_num,\n" +
                "CURRENT_DATE                            as STATISTICS_DT\n" +
                "from ods_table";
        createView(tableEnv, ICR_CREDITSCORE, "ICR_CREDITSCORE");
        //===========================================================================================================================================
        //                                                          todo ICR_SCORE_DESC
        //===========================================================================================================================================
        String ICR_SCORE_DESC = " select\n" +
                "                  PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                  PSM.PC01.PC010D01                       as score_cd,\n" +
                "                  PRH.PA01.PA01A.PA01AI01                 as SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table";
        createView(tableEnv, ICR_SCORE_DESC, "ICR_SCORE_DESC");
        //===========================================================================================================================================
        //                                                          todo ICR_CREDIT_CUE_NUM
        //===========================================================================================================================================
        String ICR_CREDIT_CUE_NUM = "select\n" +
                "                  PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                  PCO.PC02.PC02A.PC02AS01                 as acct_total_cnt,\n" +
                "                  PCO.PC02.PC02A.PC02AS02                 as busi_type_num,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table";
        createView(tableEnv, ICR_CREDIT_CUE_NUM, "ICR_CREDIT_CUE_NUM");
        //===========================================================================================================================================
        //                                                          todo ICR_CREDIT_CUE
        //===========================================================================================================================================
        String ICR_CREDIT_CUE = "select\n" +
                "                  t1.report_id                            as report_id,\n" +
                "                  info.PC02AD01                           as busi_type_cd,\n" +
                "                  info.PC02AD02                           as busi_kind_cd,\n" +
                "                  info.PC02AS03                           as acct_cnt,\n" +
                "                  info.PC02AR01                           as first_mon,\n" +
                "                  t1.SID                                  as SID,\n" +
                "                  t1.STATISTICS_DT                        as STATISTICS_DT\n" +
                "                  from\n" +
                "                  (select\n" +
                "                  PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                  PCO.PC02.PC02A.PC02AH                   as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01                 as SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table)t1,unnest(t1.data) as info(PC02AD01,PC02AD02,PC02AS03,PC02AR01) where t1.data is not null";


        createView(tableEnv, ICR_CREDIT_CUE, "ICR_CREDIT_CUE");
        //    tableEnv.sqlQuery("select * from ICR_CREDIT_CUE").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_RECOUPED_NUM
        //===========================================================================================================================================
        String ICR_RECOUPED_NUM = "select\n" +
                "                  PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                  PCO.PC02.PC02B.PC02BS01                 as rec_total_cnt,\n" +
                "                  PCO.PC02.PC02B.PC02BJ01                 as rec_total_bal,\n" +
                "                  PCO.PC02.PC02B.PC02BS02                 as rec_type_num,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table";


        createView(tableEnv, ICR_RECOUPED_NUM, "ICR_RECOUPED_NUM");


        //===========================================================================================================================================
        //                                                          todo ICR_RECOUPED_INFO
        //===========================================================================================================================================
        String ICR_RECOUPED_INFO = "select\n" +
                "                  t1.report_id                            as report_id,\n" +
                "                  info.PC02BD01                           as rec_type_cd,\n" +
                "                  cast(info.PC02BS03 as bigint)           as rec_acct_cnt,\n" +
                "                  cast(info.PC02BJ02 as decimal(18,2))    as rec_bal,\n" +
                "                  t1.SID                                  as SID,\n" +
                "                  t1.STATISTICS_DT                        as STATISTICS_DT\n" +
                "                  from (\n" +
                "                  select\n" +
                "                  PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                  PCO.PC02.PC02B.PC02BH                   as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01                 as SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PC02BD01,PC02BS03,PC02BJ02) where t1.data is not null";


        createView(tableEnv, ICR_RECOUPED_INFO, "ICR_RECOUPED_INFO");
        //    tableEnv.sqlQuery("select * from ICR_RECOUPED_INFO").toAppendStream[Row].print()

        //===========================================================================================================================================
        //                                                          todo ICR_DEADACCOUNT
        //===========================================================================================================================================
        String ICR_DEADACCOUNT = "select\n" +
                "                  PRH.PA01.PA01A.PA01AI01                         as report_id,\n" +
                "                  cast(PCO.PC02.PC02C.PC02CS01 as bigint)         as deadacct_cnt,\n" +
                "                  cast(PCO.PC02.PC02C.PC02CJ01 as decimal(18,2))  as deadacct_bal,\n" +
                "                  CURRENT_DATE                                    as STATISTICS_DT\n" +
                "                  from ods_table";
        createView(tableEnv, ICR_DEADACCOUNT, "ICR_DEADACCOUNT");
        //===========================================================================================================================================
        //                                                          todo ICR_OVERDUE_NUM
        //===========================================================================================================================================
        String ICR_OVERDUE_NUM = " select\n" +
                "                  PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                  cast(PCO.PC02D.PC02DS01 as bigint)      as ove_type_num,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table";


        createView(tableEnv, ICR_OVERDUE_NUM, "ICR_OVERDUE_NUM");

        //===========================================================================================================================================
        //                                                          todo ICR_OVERDUE
        //===========================================================================================================================================
        String ICR_OVERDUE = " select\n" +
                "                  t1.report_id                            as report_id,\n" +
                "                  info.PC02DD01                           as ove_type_cd,\n" +
                "                  info.PC02DS02                           as ove_acct_cnt,\n" +
                "                  info.PC02DS03                           as ove_mon_num,\n" +
                "                  info.PC02DJ01                           as ove_bal,\n" +
                "                  info.PC02DS04                           as ove_mon,\n" +
                "                  t1.SID                                  as SID,\n" +
                "                  t1.STATISTICS_DT                        as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                  PCO.PC02.PC02D.PC02DH                   as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01                 as SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table)t1,unnest(t1.data) as info(PC02DD01,PC02DS02,PC02DS03,PC02DJ01,PC02DS04) where t1.data is not null";
        createView(tableEnv, ICR_OVERDUE, "ICR_OVERDUE");
        //===========================================================================================================================================
        //                                                          todo ICR_ONEOFF
        //===========================================================================================================================================
        String ICR_ONEOFF = "select\n" +
                "                      PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                      cast(PCO.PC02.PC02E.PC02ES01 as bigint)                 as oneoff_org_num,\n" +
                "                      cast(PCO.PC02.PC02E.PC02ES02 as bigint)                 as oneoff_acct_cnt,\n" +
                "                      cast(PCO.PC02.PC02E.PC02EJ01 as decimal(18,2))                 as oneoff_credit_amt,\n" +
                "                      cast(PCO.PC02.PC02E.PC02EJ02 as decimal(18,2))                 as oneoff_bal,\n" +
                "                      cast(PCO.PC02.PC02E.PC02EJ03 as decimal(18,2))                 as oneoff_6mon_avg,\n" +
                "                      CURRENT_DATE                            as STATISTICS_DT\n" +
                "                        from ods_table";
        createView(tableEnv, ICR_ONEOFF, "ICR_ONEOFF");
        //===========================================================================================================================================
        //                                                          todo ICR_REVOLVING_ACCT
        //===========================================================================================================================================
        String ICR_REVOLVING_ACCT = "select\n" +
                "                      PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                      cast(PCO.PC02.PC02F.PC02FS01 as bigint)                 as revacct_org_num,\n" +
                "                      cast(PCO.PC02.PC02F.PC02FS02 as bigint)                 as revacc_acct_cnt,\n" +
                "                      cast(PCO.PC02.PC02F.PC02FJ01 as decimal(18,2))                 as revacct_credit_amt,\n" +
                "                      cast(PCO.PC02.PC02F.PC02FJ02 as decimal(18,2))                 as revacct_bal,\n" +
                "                      cast(PCO.PC02.PC02F.PC02FJ03 as decimal(18,2))                 as revacct_6mon_avg,\n" +
                "                      CURRENT_DATE                            as STATISTICS_DT\n" +
                "                        from ods_table";
        createView(tableEnv, ICR_REVOLVING_ACCT, "ICR_REVOLVING_ACCT");
        //    tableEnv.sqlQuery("select * from ICR_REVOLVING_ACCT").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_REVOLVING
        //===========================================================================================================================================
        String ICR_REVOLVING = " select\n" +
                "                      PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                      cast(PCO.PC02.PC02G.PC02GS01 as bigint)                 as rev_org_num,\n" +
                "                      cast(PCO.PC02.PC02G.PC02GS02 as bigint)                 as rev_acct_cnt,\n" +
                "                      cast(PCO.PC02.PC02G.PC02GJ01 as decimal(18,2))                 as rev_credit_amt,\n" +
                "                      cast(PCO.PC02.PC02G.PC02GJ02 as decimal(18,2))                 as rev_bal,\n" +
                "                      cast(PCO.PC02.PC02G.PC02GJ03 as decimal(18,2))                 as rev_6mon_avg,\n" +
                "                      CURRENT_DATE                            as STATISTICS_DT\n" +
                "                        from ods_table";
        createView(tableEnv, ICR_REVOLVING, "ICR_REVOLVING");
        //===========================================================================================================================================
        //                                                          todo ICR_CREDITCARD
        //===========================================================================================================================================
        String ICR_CREDITCARD = " select\n" +
                "                      PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                      cast(PCO.PC02.PC02H.PC02HS01 as bigint)                 as ccd_org_num,\n" +
                "                      cast(PCO.PC02.PC02H.PC02HS02 as bigint)                 as ccd_acct_cnt,\n" +
                "                      cast(PCO.PC02.PC02H.PC02HJ01 as decimal(18,2))                 as ccd_credit_amt,\n" +
                "                      cast(PCO.PC02.PC02H.PC02HJ02 as decimal(18,2))                 as ccd_max_bal,\n" +
                "                      cast(PCO.PC02.PC02H.PC02HJ03 as decimal(18,2))                 as ccd_min_bal,\n" +
                "                      cast(PCO.PC02.PC02H.PC02HJ04 as decimal(18,2))                 as ccd_bal,\n" +
                "                      cast(PCO.PC02.PC02H.PC02HJ05 as decimal(18,2))                 as ccd_6mon_avg,\n" +
                "                      CURRENT_DATE                            as STATISTICS_DT\n" +
                "                        from ods_table";

        createView(tableEnv, ICR_CREDITCARD, "ICR_CREDITCARD");
        //===========================================================================================================================================
        //                                                          todo ICR_QUASI_CREDITCARD
        //===========================================================================================================================================
        String ICR_QUASI_CREDITCARD = "select\n" +
                "                      PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                      cast(PCO.PC02.PC02I.PC02IS01 as bigint)                 as qua_org_num,\n" +
                "                      cast(PCO.PC02.PC02I.PC02IS02 as bigint)                 as qua_acct_cnt,\n" +
                "                      cast(PCO.PC02.PC02I.PC02IJ01 as decimal(18,2))                 as qua_credit_amtt,\n" +
                "                      cast(PCO.PC02.PC02I.PC02IJ02 as decimal(18,2))                 as qua_max_bal,\n" +
                "                      cast(PCO.PC02.PC02I.PC02IJ03 as decimal(18,2))                 as qua_min_bal,\n" +
                "                      cast(PCO.PC02.PC02I.PC02IJ04 as decimal(18,2))                 as qua_bal,\n" +
                "                      cast(PCO.PC02.PC02I.PC02IJ05 as decimal(18,2))                 as qua_6mon_avg,\n" +
                "                      CURRENT_DATE                            as STATISTICS_DT\n" +
                "                        from ods_table";
        createView(tableEnv, ICR_QUASI_CREDITCARD, "ICR_QUASI_CREDITCARD");
        //===========================================================================================================================================
        //                                                          todo ICR_REPAY_DUTY_NUM
        //===========================================================================================================================================
        String ICR_REPAY_DUTY_NUM = "select\n" +
                "                      PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                      cast(PCO.PC02.PC02K.PC02KS01 as bigint)                 as rep_duty_num,\n" +
                "                      CURRENT_DATE                            as STATISTICS_DT\n" +
                "                        from ods_table";

        createView(tableEnv, ICR_REPAY_DUTY_NUM, "ICR_REPAY_DUTY_NUM");
        //===========================================================================================================================================
        //                                                          todo ICR_POSTPAID_NUM
        //===========================================================================================================================================
        String ICR_POSTPAID_NUM = " select\n" +
                "                      PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                      cast(PNO.PC03.PC030S01 as bigint)                 as pos_type_num,\n" +
                "                      CURRENT_DATE                            as STATISTICS_DT\n" +
                "                        from ods_table";

        createView(tableEnv, ICR_POSTPAID_NUM, "ICR_POSTPAID_NUM");
        //tableEnv.sqlQuery("select * from ICR_POSTPAID_NUM").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_PUBLIC_TYPE_NUM
        //===========================================================================================================================================
        String ICR_PUBLIC_TYPE_NUM = "select\n" +
                "                      PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                      cast(PPO.PC04.PC040S01 as bigint)                 as pub_type_num,\n" +
                "                      CURRENT_DATE                            as STATISTICS_DT\n" +
                "                        from ods_table";
        createView(tableEnv, ICR_PUBLIC_TYPE_NUM, "ICR_PUBLIC_TYPE_NUM");
        //tableEnv.sqlQuery("select * from ICR_PUBLIC_TYPE_NUM").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_LAST_QUERY
        //===========================================================================================================================================
        String ICR_LAST_QUERY = " select\n" +
                "                      PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                      PQO.PC05.PC05A.PC05AR01                 as lastque_dt,\n" +
                "                      PQO.PC05.PC05A.PC05AD01                 as lastque_org_type,\n" +
                "                      PQO.PC05.PC05A.PC05AI01                 as lastque_org_id,\n" +
                "                      PQO.PC05.PC05A.PC05AQ01                 as lastque_reason,\n" +
                "                      CURRENT_DATE                            as STATISTICS_DT\n" +
                "                        from ods_table";
        createView(tableEnv, ICR_LAST_QUERY, "ICR_LAST_QUERY");
        //tableEnv.sqlQuery("select * from ICR_LAST_QUERY").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_QUERY
        //===========================================================================================================================================
        String ICR_QUERY = "select\n" +
                "                      PRH.PA01.PA01A.PA01AI01                 as report_id,\n" +
                "                      cast(PQO.PC05.PC05B.PC05BS01 as bigint)                 as que_1mon_org,\n" +
                "                      cast(PQO.PC05.PC05B.PC05BS02 as bigint)                 as que_1mon_org_ccd,\n" +
                "                      cast(PQO.PC05.PC05B.PC05BS03 as bigint)                 as que_1mon_cnt,\n" +
                "                      cast(PQO.PC05.PC05B.PC05BS04 as bigint)                 as que_1mon_cnt_ccd,\n" +
                "                      cast(PQO.PC05.PC05B.PC05BS05 as bigint)                 as que_1mon_cnt_self,\n" +
                "                      cast(PQO.PC05.PC05B.PC05BS06 as bigint)                 as que_2year_cnt,\n" +
                "                      cast(PQO.PC05.PC05B.PC05BS07 as bigint)                 as que_2year_cnt_guar,\n" +
                "                      cast(PQO.PC05.PC05B.PC05BS08 as bigint)                 as que_2year_cnt_mer,\n" +
                "                      CURRENT_DATE                            as STATISTICS_DT\n" +
                "                        from ods_table";

        createView(tableEnv, ICR_QUERY, "ICR_QUERY");
        //tableEnv.sqlQuery("select * from ICR_QUERY").toAppendStream[Row].print()

        //===========================================================================================================================================
        //                                                          todo ICR_REPAYMENT_DUTY
        //===========================================================================================================================================
        String ICR_REPAYMENT_DUTY = " select\n" +
                "                  t1.report_id                            as report_id,\n" +
                "                  info.PC02KD01                           as rep_iden_type,\n" +
                "                  info.PC02KD02                           as rep_duty_type_cd,\n" +
                "                  cast(info.PC02KS02 as bigint)           as rep_acct_cnt,\n" +
                "                  cast(info.PC02KJ01 as decimal(18,2))    as rep_amt,\n" +
                "                  cast(info.PC02KJ02 as decimal(18,2))    as rep_bal,\n" +
                "                  t1.SID                                  as SID,\n" +
                "                  t1.STATISTICS_DT                        as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PCO.PC02.PC02K.PC02KH         as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PC02KD01,PC02KD02,PC02KS02,PC02KJ01,PC02KJ02) where t1.data is not null";


        createView(tableEnv, ICR_REPAYMENT_DUTY, "ICR_REPAYMENT_DUTY");
        //tableEnv.sqlQuery("select * from ICR_REPAYMENT_DUTY").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_POSTPAID
        //===========================================================================================================================================
        String ICR_POSTPAID = " select\n" +
                "                  t1.report_id                            as report_id,\n" +
                "                  info.PC030D01                           as    pos_type_cd,\n" +
                "                  cast(info.PC030S02 as bigint)           as    pos_acct_cnt,\n" +
                "                  cast(info.PC030J01 as decimal(18,2))    as    pos_bal,\n" +
                "                  t1.SID                                  as SID,\n" +
                "                  t1.STATISTICS_DT                        as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PNO.PC030H         as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PC030D01,PC030S02,PC030J01) where t1.data is not null";

        createView(tableEnv, ICR_POSTPAID, "ICR_POSTPAID");
        //tableEnv.sqlQuery("select * from ICR_POSTPAID").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_PUBLIC
        //===========================================================================================================================================
        String ICR_PUBLIC = " select\n" +
                "                  t1.report_id                            as report_id,\n" +
                "                  info.PC040D01                           as pub_type_cd,\n" +
                "                  cast(info.PC040S02 as bigint)           as pub_cnt,\n" +
                "                  cast(info.PC040J01 as decimal(18,2))    as pub_amt,\n" +
                "                  t1.SID                                  as SID,\n" +
                "                  t1.STATISTICS_DT                        as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PPO.PC040H         as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PC040D01,PC040S02,PC040J01) where t1.data is not null";


        createView(tableEnv, ICR_PUBLIC, "ICR_PUBLIC");
        //tableEnv.sqlQuery("select * from ICR_PUBLIC").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_QUERY_RECORD
        //===========================================================================================================================================
        String ICR_QUERY_RECORD = "select\n" +
                "                  t1.report_id                            as report_id,\n" +
                "                  info.PH010R01                           as que_dt,\n" +
                "                  info.PH010D01                           as que_org_type,\n" +
                "                  info.PH010Q02                           as que_org_id,\n" +
                "                  info.PH010Q03                           as que_reason,\n" +
                "                  t1.SID                                  as SID,\n" +
                "                  t1.STATISTICS_DT                        as STATISTICS_DT\n" +
                "                  from (\n" +
                "                  select\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  POQ.PH01         as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PH010R01,PH010D01,PH010Q02,PH010Q03) where t1.data is not null";


        createView(tableEnv, ICR_QUERY_RECORD, "ICR_QUERY_RECORD");
        //tableEnv.sqlQuery("select * from ICR_QUERY_RECORD").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_LOAN_INFO
        //===========================================================================================================================================
        String ICR_LOAN_INFO = "select\n" +
                "                  t1.report_id                            as report_id,\n" +
                "                  info.PD01A.PD01AI01                     as acct_num,\n" +
                "                  info.PD01A.PD01AD01                     as acct_type_cd,\n" +
                "                  info.PD01A.PD01AD02                     as acct_org_type_cd,\n" +
                "                  info.PD01A.PD01AI02                     as acct_org_id,\n" +
                "                  info.PD01A.PD01AI03                     as acct_sign,\n" +
                "                  info.PD01A.PD01AI04                     as acct_agmt_num,\n" +
                "                  info.PD01A.PD01AD03                     as busi_spec_cd,\n" +
                "                  info.PD01A.PD01AR01                     as open_dt,\n" +
                "                  info.PD01A.PD01AD04                     as currency_cd,\n" +
                "                  cast(info.PD01A.PD01AJ01 as decimal(18,2)) as due_amt,\n" +
                "                  cast(info.PD01A.PD01AJ02 as decimal(18,2)) as credit_amt,\n" +
                "                  cast(info.PD01A.PD01AJ03 as decimal(18,2)) as credit_amt_share,\n" +
                "                  info.PD01A.PD01AR02                     as due_dt,\n" +
                "                  info.PD01A.PD01AD05                     as repay_mode_cd,\n" +
                "                  info.PD01A.PD01AD06                     as repay_frequency_cd,\n" +
                "                  cast(info.PD01A.PD01AS01 as bigint)     as repay_period,\n" +
                "                  info.PD01A.PD01AD07                     as grt_mode_cd,\n" +
                "                  info.PD01A.PD01AD08                     as lending_mode_cd,\n" +
                "                  info.PD01A.PD01AD09                     as share_sign_cd,\n" +
                "                  t1.SID                                  as SID,\n" +
                "                  t1.STATISTICS_DT                        as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PDA.PD01                                as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PD01A,PD01B,PD01C,PD01D,PD01E,PD01F,PD01G,PD01H,PD01Z) where t1.data is not null";


        createView(tableEnv, ICR_LOAN_INFO, "ICR_LOAN_INFO");
        //tableEnv.sqlQuery("select * from ICR_LOAN_INFO").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_LOAN_LATEST
        //===========================================================================================================================================
        String ICR_LOAN_LATEST = "select\n" +
                "                  t1.report_id                            as report_id,\n" +
                "                  info.PD01A.PD01AI01                     as acct_num,\n" +
                "                  info.PD01B.PD01BD01                     as acct_stat,\n" +
                "                  info.PD01B.PD01BR01                     as close_dt,\n" +
                "                  info.PD01B.PD01BR04                     as out_month,\n" +
                "                  cast(info.PD01B.PD01BJ01 as decimal(18,2))  as loan_bal,\n" +
                "                  info.PD01B.PD01BR02                     as latest_repay_dt,\n" +
                "                  cast(info.PD01B.PD01BJ02 as decimal(18,2)) as latest_repay_amt,\n" +
                "                  info.PD01B.PD01BD03                     as five_class_cd,\n" +
                "                  info.PD01B.PD01BD04                     as repay_stat,\n" +
                "                  info.PD01B.PD01BR03                     as report_dt,\n" +
                "                  t1.SID                                  as SID,\n" +
                "                  t1.STATISTICS_DT                        as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PDA.PD01                                as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PD01A,PD01B,PD01C,PD01D,PD01E,PD01F,PD01G,PD01H,PD01Z) where t1.data is not null";


        createView(tableEnv, ICR_LOAN_LATEST, "ICR_LOAN_LATEST");
        //tableEnv.sqlQuery("select * from ICR_LOAN_LATEST").toAppendStream[Row].print()

        //===========================================================================================================================================
        //                                                          todo ICR_LOAN_1MONTH
        //===========================================================================================================================================
        String ICR_LOAN_1MONTH = "select\n" +
                "                  t1.report_id                                as report_id,\n" +
                "                  info.PD01A.PD01AI01                         as acct_num,\n" +
                "                  info.PD01C.PD01CR01                         as mmonth,\n" +
                "                  info.PD01C.PD01CD01                         as acct_stat_1m,\n" +
                "                  cast(info.PD01C.PD01CJ01 as decimal(18,2))   as loan_bal_1m,\n" +
                "                  cast(info.PD01C.PD01CJ02 as decimal(18,2))   as credit_amt_used,\n" +
                "                  cast(info.PD01C.PD01CJ03 as decimal(18,2))   as credit_amt_big,\n" +
                "                  info.PD01C.PD01CD02                         as five_class_1m,\n" +
                "                  cast(info.PD01C.PD01CS01 as bigint)         as repay_period_left,\n" +
                "                  info.PD01C.PD01CR02                         as repay_date,\n" +
                "                  cast(info.PD01C.PD01CJ04 as bigint)         as repay_amt,\n" +
                "                  cast(info.PD01C.PD01CJ05 as bigint)         as repay_amt_act,\n" +
                "                  info.PD01C.PD01CR03                         as repay_date_latest,\n" +
                "                  cast(info.PD01C.PD01CS02 as bigint)         as overdue_period,\n" +
                "                  cast(info.PD01C.PD01CJ06 as decimal(18,2))  as overdue_amt,\n" +
                "                  cast(info.PD01C.PD01CJ07 as decimal(18,2))  as overdue_prin_31Amt,\n" +
                "                  cast(info.PD01C.PD01CJ08 as decimal(18,2))  as overdue_prin_61Amt,\n" +
                "                  cast(info.PD01C.PD01CJ09 as decimal(18,2))  as overdue_prin_91Amt,\n" +
                "                  cast(info.PD01C.PD01CJ10 as decimal(18,2))  as overdue_prin_180Amt,\n" +
                "                  cast(info.PD01C.PD01CJ11 as decimal(18,2))  as overdraft_prin_180Amt,\n" +
                "                  cast(info.PD01C.PD01CJ12 as decimal(18,2))  as credit_6mon_avg,\n" +
                "                  cast(info.PD01C.PD01CJ13 as decimal(18,2))  as overdraft_6mon_avg,\n" +
                "                  cast(info.PD01C.PD01CJ14 as decimal(18,2))  as max_credit_used,\n" +
                "                  cast(info.PD01C.PD01CJ15 as decimal(18,2))  as max_overdraft,\n" +
                "                  info.PD01C.PD01CR04                         as report_dt_1mon,\n" +
                "                  t1.SID                                  as SID,\n" +
                "                  t1.STATISTICS_DT                        as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PDA.PD01                                as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PD01A,PD01B,PD01C,PD01D,PD01E,PD01F,PD01G,PD01H,PD01Z) where t1.data is not null";


        createView(tableEnv, ICR_LOAN_1MONTH, "ICR_LOAN_1MONTH");
        //tableEnv.sqlQuery("select * from ICR_LOAN_1MONTH").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_LOAN_24MONTH_DATE
        //===========================================================================================================================================
        String ICR_LOAN_24MONTH_DATE = " select\n" +
                "                  t1.report_id                                as report_id,\n" +
                "                  info.PD01A.PD01AI01                         as acct_num,\n" +
                "                  info.PD01D.PD01DR01                         as start_dt,\n" +
                "                  info.PD01D.PD01DR02                         as end_dt,\n" +
                "                  t1.SID                                      as SID,\n" +
                "                  t1.STATISTICS_DT                            as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PDA.PD01                                as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PD01A,PD01B,PD01C,PD01D,PD01E,PD01F,PD01G,PD01H,PD01Z) where t1.data is not null";


        createView(tableEnv, ICR_LOAN_24MONTH_DATE, "ICR_LOAN_24MONTH_DATE");
        //tableEnv.sqlQuery("select * from ICR_LOAN_24MONTH_DATE").toAppendStream[Row].print()

        //===========================================================================================================================================
        //                                                          todo 中间表 PDAPD01
        //===========================================================================================================================================
        String PDAPD01 = " select\n" +
                "                  t1.report_id as report_id,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT,\n" +
                "                  info.PD01A  as PD01A,\n" +
                "                  info.PD01B  as PD01B,\n" +
                "                  info.PD01C  as PD01C,\n" +
                "                  info.PD01D  as PD01D,\n" +
                "                  info.PD01E  as PD01E,\n" +
                "                  info.PD01F  as PD01F,\n" +
                "                  info.PD01G  as PD01G,\n" +
                "                  info.PD01H  as PD01H,\n" +
                "                  info.PD01Z  as PD01Z\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PDA.PD01   as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PD01A,PD01B,PD01C,PD01D,PD01E,PD01F,PD01G,PD01H,PD01Z) where t1.data is not null";


        createView(tableEnv, PDAPD01, "PDAPD01");
        //tableEnv.sqlQuery("select * from PDAPD01").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo 中间表 PCAPD02
        //===========================================================================================================================================
        String PCAPD02 = "select\n" +
                "                  t1.report_id as report_id,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT,\n" +
                "                  info.PD02A as PD02A,\n" +
                "                  info.PD02Z as PD02Z\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PCA.PD02   as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PD02A,PD02Z) where t1.data is not null";


        createView(tableEnv, PCAPD02, "PCAPD02");
        //tableEnv.sqlQuery("select * from PCAPD02").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo 中间表 PCRPD03
        //===========================================================================================================================================
        String PCRPD03 = "select\n" +
                "                  t1.report_id as report_id,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT,\n" +
                "                  info.PD03A as PD03A,\n" +
                "                  info.PD03Z as PD03Z\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PCR.PD03   as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PD03A,PD03Z) where t1.data is not null";


        createView(tableEnv, PCRPD03, "PCRPD03");
        //===========================================================================================================================================
        //                                                          todo 中间表 PNDPE01
        //===========================================================================================================================================
        String PNDPE01 = "select\n" +
                "                  t1.report_id as report_id,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT,\n" +
                "                  info.PE01A as PE01A,\n" +
                "                  info.PE01Z as PE01Z\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PND.PE01   as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PE01A,PE01Z) where t1.data is not null";


        createView(tableEnv, PNDPE01, "PNDPE01");

        //===========================================================================================================================================
        //                                                          todo 中间表 POTPF01
        //===========================================================================================================================================
        String POTPF01 = "select\n" +
                "                  t1.report_id as report_id,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT,\n" +
                "                  info.PF01A as PF01A,\n" +
                "                  info.PF01Z as PF01Z\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  POT.PF01   as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PF01A,PF01Z) where t1.data is not null";


        createView(tableEnv, POTPF01, "POTPF01");


        //===========================================================================================================================================
        //                                                          todo 中间表 PCJPF02
        //===========================================================================================================================================
        String PCJPF02 = "select\n" +
                "                  t1.report_id as report_id,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT,\n" +
                "                  info.PF02A as PF02A,\n" +
                "                  info.PF02Z as PF02Z\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PCJ.PF02   as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PF02A,PF02Z) where t1.data is not null";


        createView(tableEnv, PCJPF02, "PCJPF02");
        //===========================================================================================================================================
        //                                                          todo 中间表 PCEPF03
        //===========================================================================================================================================
        String PCEPF03 = "select\n" +
                "                  t1.report_id as report_id,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT,\n" +
                "                  info.PF03A as PF03A,\n" +
                "                  info.PF03Z as PF03Z\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PCE.PF03   as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PF03A,PF03Z) where t1.data is not null";


        createView(tableEnv, PCEPF03, "PCEPF03");
        //===========================================================================================================================================
        //                                                          todo 中间表 PAPPF04
        //===========================================================================================================================================
        String PAPPF04 = " select\n" +
                "                  t1.report_id as report_id,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT,\n" +
                "                  info.PF04A as PF04A,\n" +
                "                  info.PF04Z as PF04Z\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PAP.PF04   as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PF04A,PF04Z) where t1.data is not null";


        createView(tableEnv, PAPPF04, "PAPPF04");
        //===========================================================================================================================================
        //                                                          todo 中间表 PHFPF05
        //===========================================================================================================================================
        String PHFPF05 = "select\n" +
                "                  t1.report_id as report_id,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT,\n" +
                "                  info.PF05A as PF05A,\n" +
                "                  info.PF05Z as PF05Z\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PHF.PF05   as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PF05A,PF05Z) where t1.data is not null";


        createView(tableEnv, PHFPF05, "PHFPF05");

        //===========================================================================================================================================
        //                                                          todo 中间表 PBSPF06
        //===========================================================================================================================================

        String PBSPF06 = "select\n" +
                "                  t1.report_id as report_id,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT,\n" +
                "                  info.PF06A as PF06A,\n" +
                "                  info.PF06Z as PF06Z\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PBS.PF06   as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PF06A,PF06Z) where t1.data is not null";


        createView(tableEnv, PBSPF06, "PBSPF06");
        //===========================================================================================================================================
        //                                                          todo 中间表 PPQPF07
        //===========================================================================================================================================

        String PPQPF07 = "select\n" +
                "                  t1.report_id as report_id,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT,\n" +
                "                  info.PF07A as PF07A,\n" +
                "                  info.PF07Z as PF07Z\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PPQ.PF07   as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PF07A,PF07Z) where t1.data is not null";


        createView(tableEnv, PPQPF07, "PPQPF07");
        //===========================================================================================================================================
        //                                                          todo 中间表 PAHPF08
        //===========================================================================================================================================
        String PAHPF08 = "select\n" +
                "                  t1.report_id as report_id,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT,\n" +
                "                  info.PF08A as PF08A,\n" +
                "                  info.PF08Z as PF08Z\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PAH.PF08   as data,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "                  PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "                  CURRENT_DATE                            as STATISTICS_DT\n" +
                "                  from ods_table\n" +
                "                  )t1,unnest(t1.data) as info(PF08A,PF08Z) where t1.data is not null";


        createView(tableEnv, PAHPF08, "PAHPF08");

        //===========================================================================================================================================
        //                                                          todo ICR_LOAN_24MONTH
        //===========================================================================================================================================
        String ICR_LOAN_24MONTH = "select\n" +
                "                  t1.report_id                                    as report_id,\n" +
                "                  t1.acct_num                                     as acct_num,\n" +
                "                  info.PD01DR03                                   as month_24m,\n" +
                "                  info.PD01DD01                                   as repay_stat_24m,\n" +
                "                  t1.SID  \t\t\t\t                        as SID,\n" +
                "                  t1.STATISTICS_DT                                as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PD01A.PD01AI01 as acct_num,\n" +
                "                  report_id as report_id,\n" +
                "                  SID as SID,\n" +
                "                  STATISTICS_DT as  STATISTICS_DT,\n" +
                "                  PD01D.PD01DH as data\n" +
                "                  from PDAPD01\n" +
                "                  )t1,unnest(t1.data) as info(PD01DR03,PD01DD01) where t1.data is not null";


        createView(tableEnv, ICR_LOAN_24MONTH, "ICR_LOAN_24MONTH");
        //    tableEnv.sqlQuery("select * from ICR_LOAN_24MONTH").toAppendStream[Row].print()


        //===========================================================================================================================================
        //                                                          todo ICR_LOAN_5YEARS_DATE
        //===========================================================================================================================================
        String ICR_LOAN_5YEARS_DATE = "select\n" +
                "                  report_id as report_id,\n" +
                "                  PD01A.PD01AI01 as acct_num,\n" +
                "                  PD01E.PD01ER01 as start_dt_5y,\n" +
                "                  PD01E.PD01ER02 as end_dt_5y,\n" +
                "                  cast(PD01E.PD01ES01 as bigint) as mon_num,\n" +
                "                  SID as SID,\n" +
                "                  STATISTICS_DT as STATISTICS_DT\n" +
                "                  from PDAPD01";


        createView(tableEnv, ICR_LOAN_5YEARS_DATE, "ICR_LOAN_5YEARS_DATE");
        //tableEnv.sqlQuery("select * from ICR_LOAN_5YEARS_DATE").toAppendStream[Row].print()


        //===========================================================================================================================================
        //                                                          todo ICR_LOAN_5YEARS
        //===========================================================================================================================================
        String ICR_LOAN_5YEARS = "select\n" +
                "                  t1.report_id                                    as report_id,\n" +
                "                  t1.acct_num                                     as acct_num,\n" +
                "                  info.PD01ER03                                   as month_5y,\n" +
                "                  info.PD01ED01                                   as repay_stat_5y,\n" +
                "                  cast(info.PD01EJ01 as decimal(18,2))            as amt_5y,\n" +
                "                  t1.SID  \t\t\t\t                        as SID,\n" +
                "                  t1.STATISTICS_DT                                as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PD01A.PD01AI01      as acct_num,\n" +
                "                  report_id \t\t\tas report_id,\n" +
                "                  SID \t\t\t\tas SID,\n" +
                "                  STATISTICS_DT \t\tas STATISTICS_DT,\n" +
                "                  PD01E.PD01EH        as data\n" +
                "                  from PDAPD01\n" +
                "                  )t1,unnest(t1.data) as info(PD01ER03,PD01ED01,PD01EJ01) where t1.data is not null";


        createView(tableEnv, ICR_LOAN_5YEARS, "ICR_LOAN_5YEARS");
        //tableEnv.sqlQuery("select * from ICR_LOAN_5YEARS").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_SPECIAL_TXN_NUM
        //===========================================================================================================================================
        String ICR_SPECIAL_TXN_NUM = "select\n" +
                "                  report_id as report_id,\n" +
                "                  PD01A.PD01AI01      as acct_num,\n" +
                "                  cast(PD01F.PD01FS01 as bigint) as spe_txn_num,\n" +
                "                  SID as SID,\n" +
                "                  STATISTICS_DT as STATISTICS_DT\n" +
                "                  from PDAPD01";


        createView(tableEnv, ICR_SPECIAL_TXN_NUM, "ICR_SPECIAL_TXN_NUM");
        //tableEnv.sqlQuery("select * from ICR_SPECIAL_TXN_NUM").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_SPECIAL_TXN
        //===========================================================================================================================================
        String ICR_SPECIAL_TXN = "select\n" +
                "                  t1.report_id as report_id,\n" +
                "                  t1.acct_num as acct_num,\n" +
                "                  info.PD01FD01 as spe_txn_type_cd,\n" +
                "                  info.PD01FR01 as spe_txn_dt,\n" +
                "                  cast(info.PD01FS02 as bigint) as spe_end_dt_change,\n" +
                "                  cast(info.PD01FJ01 as decimal(18,2)) as spe_amt,\n" +
                "                  info.PD01FQ01 as spe_record,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  PD01A.PD01AI01      as acct_num,\n" +
                "                  report_id \t\t\tas report_id,\n" +
                "                  SID \t\t\t\tas SID,\n" +
                "                  STATISTICS_DT \t\tas STATISTICS_DT,\n" +
                "                  PD01F.PD01FH     as data\n" +
                "                  from PDAPD01\n" +
                "                  )t1,unnest(t1.data) as info(PD01FD01,PD01FR01,PD01FS02,PD01FJ01,PD01FQ01) where t1.data is not null";


        createView(tableEnv, ICR_SPECIAL_TXN, "ICR_SPECIAL_TXN");
        //tableEnv.sqlQuery("select * from ICR_SPECIAL_TXN").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_SPECIAL_EVENT_NUM
        //===========================================================================================================================================
        String ICR_SPECIAL_EVENT_NUM = "select\n" +
                "                  report_id as report_id,\n" +
                "                  PD01A.PD01AI01      as acct_num,\n" +
                "                  cast(PD01G.PD01GS01 as bigint) as spe_event_num,\n" +
                "                  SID as SID,\n" +
                "                  STATISTICS_DT as STATISTICS_DT\n" +
                "                  from PDAPD01";


        createView(tableEnv, ICR_SPECIAL_EVENT_NUM, "ICR_SPECIAL_EVENT_NUM");
        //tableEnv.sqlQuery("select * from ICR_SPECIAL_EVENT_NUM").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_SPECIAL_EVENT
        //===========================================================================================================================================
        String ICR_SPECIAL_EVENT = " select\n" +
                "                  t1.report_id as report_id,\n" +
                "                  t1.acct_num as acct_num,\n" +
                "                  info.PD01GR01 as spe_event_dt,\n" +
                "                  info.PD01GD01 as spe_event_type,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  report_id \t\t\tas report_id,\n" +
                "                  PD01A.PD01AI01      as acct_num,\n" +
                "                  SID \t\t\t\tas SID,\n" +
                "                  STATISTICS_DT \t\tas STATISTICS_DT,\n" +
                "                  PD01G.PD01GH     as data\n" +
                "                  from PDAPD01\n" +
                "                  )t1,unnest(t1.data) as info(PD01GR01,PD01GD01) where t1.data is not null";


        createView(tableEnv, ICR_SPECIAL_EVENT, "ICR_SPECIAL_EVENT");
        //tableEnv.sqlQuery("select * from ICR_SPECIAL_EVENT").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_LARGE_INSTALMENTS_NUM
        //===========================================================================================================================================
        String ICR_LARGE_INSTALMENTS_NUM = "select\n" +
                "                  report_id as report_id,\n" +
                "                  PD01A.PD01AI01      as acct_num,\n" +
                "                  cast(PD01H.PD01HS01 as bigint) as large_cnt,\n" +
                "                  SID as SID,\n" +
                "                  STATISTICS_DT as STATISTICS_DT\n" +
                "                  from PDAPD01";


        createView(tableEnv, ICR_LARGE_INSTALMENTS_NUM, "ICR_LARGE_INSTALMENTS_NUM");
        //tableEnv.sqlQuery("select * from ICR_LARGE_INSTALMENTS_NUM").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_LARGE_INSTALMENTS
        //===========================================================================================================================================
        String ICR_LARGE_INSTALMENTS = " select\n" +
                "                  t1.report_id as report_id,\n" +
                "                  t1.acct_num as acct_num,\n" +
                "                  cast(info.PD01HJ01 as decimal(18,2)) as large_amt,\n" +
                "                  info.PD01HR01 as large_eff_dt,\n" +
                "                  info.PD01HR02 as large_end_dt,\n" +
                "                  cast(info.PD01HJ02 as decimal(18,2)) as large_amt_used,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  report_id \t\t\tas report_id,\n" +
                "                  PD01A.PD01AI01      as acct_num,\n" +
                "                  SID \t\t\t\tas SID,\n" +
                "                  STATISTICS_DT \t\tas STATISTICS_DT,\n" +
                "                  PD01H.PD01HH     as data\n" +
                "                  from PDAPD01\n" +
                "                  )t1,unnest(t1.data) as info(PD01HJ01,PD01HR01,PD01HR02,PD01HJ02) where t1.data is not null";


        createView(tableEnv, ICR_LARGE_INSTALMENTS, "ICR_LARGE_INSTALMENTS");
        //tableEnv.sqlQuery("select * from ICR_LARGE_INSTALMENTS").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_LOAN_DECLARE_NUM
        //===========================================================================================================================================
        String ICR_LOAN_DECLARE_NUM = " select\n" +
                "                  report_id as report_id,\n" +
                "                  PD01A.PD01AI01      as acct_num,\n" +
                "                  cast(PD01Z.PD01ZS01 as bigint) as declare_num,\n" +
                "                  SID as SID,\n" +
                "                  STATISTICS_DT as STATISTICS_DT\n" +
                "                  from PDAPD01";


        createView(tableEnv, ICR_LOAN_DECLARE_NUM, "ICR_LOAN_DECLARE_NUM");
        //    tableEnv.sqlQuery("select * from ICR_LOAN_DECLARE_NUM").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_LOAN_DECLARE
        //===========================================================================================================================================
        String ICR_LOAN_DECLARE = "select\n" +
                "                  t1.report_id as report_id,\n" +
                "                  t1.acct_num as acct_num,\n" +
                "                  info.PD01ZD01 as declare_type_cd,\n" +
                "                  info.PD01ZQ01 as declare_content,\n" +
                "                  info.PD01ZR01 as declare_dt,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  report_id \t\t\tas report_id,\n" +
                "                  PD01A.PD01AI01      as acct_num,\n" +
                "                  SID \t\t\t\tas SID,\n" +
                "                  STATISTICS_DT \t\tas STATISTICS_DT,\n" +
                "                  PD01Z.PD01ZH     as data\n" +
                "                  from PDAPD01\n" +
                "                  )t1,unnest(t1.data) as info(PD01ZD01,PD01ZQ01,PD01ZR01) where t1.data is not null";


        createView(tableEnv, ICR_LOAN_DECLARE, "ICR_LOAN_DECLARE");
        //tableEnv.sqlQuery("select * from ICR_LOAN_DECLARE").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_CREDIT_AGREEMENT
        //===========================================================================================================================================
        String ICR_CREDIT_AGREEMENT = "select\n" +
                "                  report_id as report_id,\n" +
                "                  PD02A.PD02AI01 as agmt_num,\n" +
                "                  PD02A.PD02AD01  as agmt_org_type,\n" +
                "                  PD02A.PD02AI02  as agmt_org_id,\n" +
                "                  PD02A.PD02AI03  as agmt_sign,\n" +
                "                  PD02A.PD02AD02  as agmt_use_cd,\n" +
                "                  cast(PD02A.PD02AJ01 as decimal(18,2))  as agmt_amt,\n" +
                "                  PD02A.PD02AD03  as agmt_currency_cd,\n" +
                "                  PD02A.PD02AR01  as agmt_eff_dt,\n" +
                "                  PD02A.PD02AR02  as agmt_due_dt,\n" +
                "                  PD02A.PD02AD04  as agmt_stat_cd,\n" +
                "                  cast(PD02A.PD02AJ04 as decimal(18,2))  as agmt_amt_used,\n" +
                "                  PD02A.PD02AI04  as agmt_limit_id,\n" +
                "                  cast(PD02A.PD02AJ03 as decimal(18,2))  as agmt_limit,\n" +
                "                  SID as SID,\n" +
                "                  STATISTICS_DT as STATISTICS_DT\n" +
                "                  from PCAPD02";


        createView(tableEnv, ICR_CREDIT_AGREEMENT, "ICR_CREDIT_AGREEMENT");
        //    tableEnv.sqlQuery("select * from ICR_CREDIT_AGREEMENT").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_AGREEMENT_DECLARE_NUM
        //===========================================================================================================================================
        String ICR_AGREEMENT_DECLARE_NUM = "select\n" +
                "                  report_id as report_id,\n" +
                "                  PD02A.PD02AI01 as agmt_num,\n" +
                "                  cast(PD02Z.PD02ZS01 as bigint) as agmt_dec_num,\n" +
                "                  SID as SID,\n" +
                "                  STATISTICS_DT as STATISTICS_DT\n" +
                "                  from PCAPD02";


        createView(tableEnv, ICR_AGREEMENT_DECLARE_NUM, "ICR_AGREEMENT_DECLARE_NUM");
        //tableEnv.sqlQuery("select * from ICR_AGREEMENT_DECLARE_NUM").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_AGREEMENT_DECLARE
        //===========================================================================================================================================
        String ICR_AGREEMENT_DECLARE = "select\n" +
                "                  t1.report_id as report_id,\n" +
                "                  t1.agmt_num as agmt_num,\n" +
                "                  info.PD02ZD01 as agmt_decl_type,\n" +
                "                  info.PD02ZQ01 as agmt_decl_cont,\n" +
                "                  info.PD02ZR01 as agmt_decl_dt,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  report_id \t\t\tas report_id,\n" +
                "                  PD02A.PD02AI01      as agmt_num,\n" +
                "                  SID \t\t\t\tas SID,\n" +
                "                  STATISTICS_DT \t\tas STATISTICS_DT,\n" +
                "                  PD02Z.PD02ZH     as data\n" +
                "                  from PCAPD02\n" +
                "                  )t1,unnest(t1.data) as info(PD02ZD01,PD02ZQ01,PD02ZR01) where t1.data is not null";


        createView(tableEnv, ICR_AGREEMENT_DECLARE, "ICR_AGREEMENT_DECLARE");
        //    tableEnv.sqlQuery("select * from ICR_AGREEMENT_DECLARE").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_POSTPAID_INFO
        //===========================================================================================================================================
        String ICR_POSTPAID_INFO = " select\n" +
                "                  report_id as report_id,\n" +
                "                  PE01A.PE01AD01 as postpaid_acct_type_cd,\n" +
                "                  PE01A.PE01AQ01 as postpaid_org_name,\n" +
                "                  PE01A.PE01AD02 as postpaid_busi_type_cd,\n" +
                "                  PE01A.PE01AR01 as postpaid_open_dt,\n" +
                "                  PE01A.PE01AD03 as postpaid_stat_cd,\n" +
                "                  cast(PE01A.PE01AJ01 as decimal(18,2)) as postpaid_owe_amt,\n" +
                "                  PE01A.PE01AR02 as postpaid_acct_mon,\n" +
                "                  PE01A.PE01AQ02 as postpaid_24mon,\n" +
                "                  STATISTICS_DT as STATISTICS_DT,\n" +
                "                  SID as SID\n" +
                "                  from PNDPE01";


        createView(tableEnv, ICR_POSTPAID_INFO, "ICR_POSTPAID_INFO");
        //tableEnv.sqlQuery("select * from ICR_POSTPAID_INFO").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_POSTPAID_DECLARE_NUM
        //===========================================================================================================================================
        String ICR_POSTPAID_DECLARE_NUM = " select\n" +
                "                  report_id as report_id,\n" +
                "                  cast(PE01Z.PE01ZS01 as bigint) as postpaid_decl_num,\n" +
                "                  STATISTICS_DT as STATISTICS_DT\n" +
                "                  from PNDPE01";


        createView(tableEnv, ICR_POSTPAID_DECLARE_NUM, "ICR_POSTPAID_DECLARE_NUM");
        //tableEnv.sqlQuery("select * from ICR_POSTPAID_DECLARE_NUM").toAppendStream[Row].print()

        //===========================================================================================================================================
        //                                                          todo ICR_TAX_INFO
        //===========================================================================================================================================
        String ICR_TAX_INFO = "select\n" +
                "                  report_id as report_id,\n" +
                "                  PF01A.PF01AQ01 as tax_org_id,\n" +
                "                  cast(PF01A.PF01AJ01 as decimal(18,2)) as tax_owe_amt,\n" +
                "                  PF01A.PF01AR01 as tax_owe_dt,\n" +
                "                  STATISTICS_DT as STATISTICS_DT\n" +
                "                  from POTPF01";


        createView(tableEnv, ICR_TAX_INFO, "ICR_TAX_INFO");
        //tableEnv.sqlQuery("select * from ICR_TAX_INFO").toAppendStream[Row].print()

        //===========================================================================================================================================
        //                                                          todo ICR_TAX_DECLARE_NUM
        //===========================================================================================================================================
        String ICR_TAX_DECLARE_NUM = " select\n" +
                "                  report_id as report_id,\n" +
                "                  cast(PF01Z.PF01ZS01 as bigint) as tax_owe_amt,\n" +
                "                  STATISTICS_DT as STATISTICS_DT\n" +
                "                  from POTPF01";


        createView(tableEnv, ICR_TAX_DECLARE_NUM, "ICR_TAX_DECLARE_NUM");
        //    tableEnv.sqlQuery("select * from ICR_TAX_DECLARE_NUM").toAppendStream[Row].print()

        //===========================================================================================================================================
        //                                                          todo ICR_CIVIL_JUDGEMENT
        //===========================================================================================================================================
        String ICR_CIVIL_JUDGEMENT = " select\n" +
                "                  report_id as report_id,\n" +
                "                  PF02A.PF02AQ01 as civ_court_id,\n" +
                "                  PF02A.PF02AQ02 as civ_reason,\n" +
                "                  PF02A.PF02AR01 as civ_reg_dt,\n" +
                "                  PF02A.PF02AD01 as civ_closed_cd,\n" +
                "                  PF02A.PF02AQ03 as civ_result,\n" +
                "                  PF02A.PF02AR02 as civ_eff_dt,\n" +
                "                  PF02A.PF02AQ04 as civ_target,\n" +
                "                  cast(PF02A.PF02AJ01 as decimal(18,2)) as civ_amt,\n" +
                "                  STATISTICS_DT as STATISTICS_DT,\n" +
                "                  SID as SID\n" +
                "                  from PCJPF02";


        createView(tableEnv, ICR_CIVIL_JUDGEMENT, "ICR_CIVIL_JUDGEMENT");
        //    tableEnv.sqlQuery("select * from ICR_CIVIL_JUDGEMENT_DECLARE").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_CIV_JUDGE_DECLARE_NUM
        //===========================================================================================================================================
        String ICR_CIV_JUDGE_DECLARE_NUM = " select\n" +
                "                  report_id as report_id,\n" +
                "                  cast(PF02Z.PF02ZS01 as bigint) as civ_decl_num,\n" +
                "                  STATISTICS_DT as STATISTICS_DT\n" +
                "                  from PCJPF02";


        createView(tableEnv, ICR_CIV_JUDGE_DECLARE_NUM, "ICR_CIV_JUDGE_DECLARE_NUM");
        //    tableEnv.sqlQuery("select * from ICR_CIV_JUDGE_DECLARE_NUM").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_ENFORCEMENT
        //===========================================================================================================================================
        String ICR_ENFORCEMENT = " select\n" +
                "                  report_id as report_id,\n" +
                "                  PF03A.PF03AQ01 as   enf_court_id,\n" +
                "                  PF03A.PF03AQ02 as   enf_reason,\n" +
                "                  PF03A.PF03AR01 as   enf_reg_dt,\n" +
                "                  PF03A.PF03AD01 as   enf_closed_cd,\n" +
                "                  PF03A.PF03AQ03 as   enf_stat,\n" +
                "                  PF03A.PF03AR02 as   enf_closed_dt,\n" +
                "                  PF03A.PF03AQ04 as   enf_apply_target,\n" +
                "                  cast(PF03A.PF03AJ01 as decimal(18,2)) as   enf_apply_amt,\n" +
                "                  PF03A.PF03AQ05 as   enf_execute_target,\n" +
                "                  cast(PF03A.PF03AJ02 as decimal(18,2)) as   enf_execute_amt,\n" +
                "                  STATISTICS_DT as STATISTICS_DT,\n" +
                "                  SID as SID\n" +
                "                  from PCEPF03";


        createView(tableEnv, ICR_ENFORCEMENT, "ICR_ENFORCEMENT");
        // tableEnv.sqlQuery("select * from ICR_ENFORCEMENT").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_ENFORCEMENT_DECLARE_NUM
        //===========================================================================================================================================
        String ICR_ENFORCEMENT_DECLARE_NUM = "select\n" +
                "                  report_id as report_id,\n" +
                "                  cast(PF03Z.PF03ZS01 as decimal(18,2)) as enf_decl_num,\n" +
                "                  STATISTICS_DT as STATISTICS_DT\n" +
                "                  from PCEPF03";


        createView(tableEnv, ICR_ENFORCEMENT_DECLARE_NUM, "ICR_ENFORCEMENT_DECLARE_NUM");
        //    tableEnv.sqlQuery("select * from ICR_ENFORCEMENT_DECLARE_NUM").toAppendStream[Row].print()

        //===========================================================================================================================================
        //                                                          todo ICR_ADMINPUNISHMENT
        //===========================================================================================================================================
        String ICR_ADMINPUNISHMENT = "select\n" +
                "                  report_id as report_id,\n" +
                "                  PF04A.PF04AQ01 as punish_org_id,\n" +
                "                  PF04A.PF04AQ02 as punish_cont,\n" +
                "                  cast(PF04A.PF04AJ01 as decimal(18,2)) as punish_amt,\n" +
                "                  PF04A.PF04AR01 as punish_eff_dt,\n" +
                "                  PF04A.PF04AR02 as punish_end_dt,\n" +
                "                  PF04A.PF04AQ03 as punish_result,\n" +
                "                  STATISTICS_DT as STATISTICS_DT,\n" +
                "                  SID as SID\n" +
                "                  from PAPPF04";


        createView(tableEnv, ICR_ADMINPUNISHMENT, "ICR_ADMINPUNISHMENT");
        //   tableEnv.sqlQuery("select * from ICR_ADMINPUNISHMENT").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_PUNISHMENT_DECLARE_NUM
        //===========================================================================================================================================
        String ICR_PUNISHMENT_DECLARE_NUM = " select\n" +
                "                  report_id as report_id,\n" +
                "                  cast(PF04Z.PF04ZS01 as bigint) as punish_decl_num,\n" +
                "                  STATISTICS_DT as STATISTICS_DT\n" +
                "                  from PAPPF04";


        createView(tableEnv, ICR_PUNISHMENT_DECLARE_NUM, "ICR_PUNISHMENT_DECLARE_NUM");
        //    tableEnv.sqlQuery("select * from ICR_PUNISHMENT_DECLARE_NUM").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_HOUSINGFUND
        //===========================================================================================================================================
        String ICR_HOUSINGFUND = "select\n" +
                "                  report_id as report_id,\n" +
                "                  PF05A.PF05AQ01 as housing_area,\n" +
                "                  PF05A.PF05AR01 as housing_dt,\n" +
                "                  PF05A.PF05AD01 as housing_stat_cd,\n" +
                "                  PF05A.PF05AR02 as housing_start_dt,\n" +
                "                  PF05A.PF05AR03 as housing_last_dt,\n" +
                "                  cast(PF05A.PF05AQ02 as bigint) as housing_prop,\n" +
                "                  cast(PF05A.PF05AQ03 as bigint) as housing_prop_indiv,\n" +
                "                  cast(PF05A.PF05AJ01 as bigint) as housing_amt,\n" +
                "                  PF05A.PF05AQ04 as housing_unit,\n" +
                "                  PF05A.PF05AR04 as housing_update_dt,\n" +
                "                  STATISTICS_DT as STATISTICS_DT,\n" +
                "                  SID as SID\n" +
                "                  from PHFPF05";


        createView(tableEnv, ICR_HOUSINGFUND, "ICR_HOUSINGFUND");
        //    tableEnv.sqlQuery("select * from ICR_HOUSINGFUND").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_HOUSINGFUND_DECLARE_NUM
        //===========================================================================================================================================
        String ICR_HOUSINGFUND_DECLARE_NUM = " select\n" +
                "                  report_id as report_id,\n" +
                "                  cast(PF05Z.PF05ZS01 as bigint) as housing_decl_num,\n" +
                "                  STATISTICS_DT as STATISTICS_DT\n" +
                "                  from PHFPF05";


        createView(tableEnv, ICR_HOUSINGFUND_DECLARE_NUM, "ICR_HOUSINGFUND_DECLARE_NUM");
        //    tableEnv.sqlQuery("select * from ICR_HOUSINGFUND_DECLARE_NUM").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_ALLOWANCE
        //===========================================================================================================================================
        String ICR_ALLOWANCE = "select\n" +
                "                  report_id as report_id,\n" +
                "                  PF06A.PF06AD01 as   allowance_type_cd,\n" +
                "                  PF06A.PF06AQ01 as   allowance_area,\n" +
                "                  PF06A.PF06AQ02 as   allowance_unit,\n" +
                "                  cast(PF06A.PF06AQ03 as bigint) as   allowance_income,\n" +
                "                  PF06A.PF06AR01 as   allowance_sup_dt,\n" +
                "                  PF06A.PF06AR02 as   allowance_app_dt,\n" +
                "                  PF06A.PF06AR03 as   allowance_update_dt,\n" +
                "                  STATISTICS_DT as STATISTICS_DT,\n" +
                "                  SID as SID\n" +
                "                  from PBSPF06";


        createView(tableEnv, ICR_ALLOWANCE, "ICR_ALLOWANCE");
        //    tableEnv.sqlQuery("select * from ICR_ALLOWANCE").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_ALLOWANCE_DECLARE_NUM
        //===========================================================================================================================================
        String ICR_ALLOWANCE_DECLARE_NUM = "select\n" +
                "                  report_id as report_id,\n" +
                "                  cast(PF06Z.PF06ZS01 as bigint) as allowance_decl_num,\n" +
                "                  STATISTICS_DT as STATISTICS_DT\n" +
                "                  from PBSPF06";


        createView(tableEnv, ICR_ALLOWANCE_DECLARE_NUM, "ICR_ALLOWANCE_DECLARE_NUM");
        //    tableEnv.sqlQuery("select * from ICR_ALLOWANCE_DECLARE_NUM").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_QUALIFICATION
        //===========================================================================================================================================
        String ICR_QUALIFICATION = " select\n" +
                "                  report_id      as report_id,\n" +
                "                  PF07A.PF07AQ01 as   qual_name,\n" +
                "                  PF07A.PF07AQ02 as   qual_org,\n" +
                "                  PF07A.PF07AD01 as   qual_level_cd,\n" +
                "                  PF07A.PF07AD02 as   qual_area,\n" +
                "                  PF07A.PF07AR01 as   qual_get_dt,\n" +
                "                  PF07A.PF07AR02 as   qual_due_dt,\n" +
                "                  PF07A.PF07AR03 as   qual_revoke_dt,\n" +
                "                  STATISTICS_DT as STATISTICS_DT,\n" +
                "                  SID as SID\n" +
                "                  from PPQPF07";


        createView(tableEnv, ICR_QUALIFICATION, "ICR_QUALIFICATION");
        //    tableEnv.sqlQuery("select * from ICR_QUALIFICATION").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_QUALFICATION_DECL_NUM
        //===========================================================================================================================================
        String ICR_QUALFICATION_DECL_NUM = " select\n" +
                "                  report_id as report_id,\n" +
                "                  cast(PF07Z.PF07ZS01 as bigint) as allowance_decl_num,\n" +
                "                  STATISTICS_DT as STATISTICS_DT\n" +
                "                  from PPQPF07";


        createView(tableEnv, ICR_QUALFICATION_DECL_NUM, "ICR_QUALFICATION_DECL_NUM");
        //    tableEnv.sqlQuery("select * from ICR_QUALFICATION_DECL_NUM").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_REWARD
        //===========================================================================================================================================
        String ICR_REWARD = "select\n" +
                "                  report_id      as   report_id,\n" +
                "                  PF08A.PF08AQ01 as   rew_org,\n" +
                "                  PF08A.PF08AQ02 as   rew_cont,\n" +
                "                  PF08A.PF08AR01 as   rew_eff_dt,\n" +
                "                  PF08A.PF08AR02 as   rew_end_dt,\n" +
                "                  STATISTICS_DT as STATISTICS_DT,\n" +
                "                  SID as SID\n" +
                "                  from PAHPF08";


        createView(tableEnv, ICR_REWARD, "ICR_REWARD");
        //    tableEnv.sqlQuery("select * from ICR_REWARD").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_REWARD_DECLARE_NUM
        //===========================================================================================================================================
        String ICR_REWARD_DECLARE_NUM = " select\n" +
                "                  report_id as report_id,\n" +
                "                  cast(PF08Z.PF08ZS01 as bigint) as allowance_decl_num,\n" +
                "                  STATISTICS_DT as STATISTICS_DT\n" +
                "                  from PAHPF08";


        createView(tableEnv, ICR_REWARD_DECLARE_NUM, "ICR_REWARD_DECLARE_NUM");
        //    tableEnv.sqlQuery("select * from ICR_REWARD_DECLARE_NUM").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_POSTPAID_DECLARE
        //===========================================================================================================================================
        String ICR_POSTPAID_DECLARE = "select\n" +
                "                  t1.report_id  as  report_id,\n" +
                "                  info.PE01ZD01 as  postpaid_decl_type,\n" +
                "                  info.PE01ZQ01 as  postpaid_decl_cont,\n" +
                "                  info.PE01ZR01 as  postpaid_decl_dt,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  report_id \t\t\tas report_id,\n" +
                "                  SID \t\t\t\tas SID,\n" +
                "                  STATISTICS_DT \t\tas STATISTICS_DT,\n" +
                "                  PE01Z.PE01ZH     as data\n" +
                "                  from PNDPE01\n" +
                "                  )t1,unnest(t1.data) as info(PE01ZD01,PE01ZQ01,PE01ZR01) where t1.data is not null";


        createView(tableEnv, ICR_POSTPAID_DECLARE, "ICR_POSTPAID_DECLARE");
        //    tableEnv.sqlQuery("select * from ICR_POSTPAID_DECLARE").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_TAX_DECLARE
        //===========================================================================================================================================
        String ICR_TAX_DECLARE = "select\n" +
                "                  t1.report_id  as  report_id,\n" +
                "                  info.PF01ZD01 as tax_decl_type,\n" +
                "                  info.PF01ZQ01 as tax_decl_cont,\n" +
                "                  info.PF01ZR01 as tax_decl_dt,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  report_id \t\t\tas report_id,\n" +
                "                  SID \t\t\t\tas SID,\n" +
                "                  STATISTICS_DT \t\tas STATISTICS_DT,\n" +
                "                  PF01Z.PF01ZH     as data\n" +
                "                  from POTPF01\n" +
                "                  )t1,unnest(t1.data) as info(PF01ZD01,PF01ZQ01,PF01ZR01) where t1.data is not null";


        createView(tableEnv, ICR_TAX_DECLARE, "ICR_TAX_DECLARE");
        //    tableEnv.sqlQuery("select * from ICR_TAX_DECLARE").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_CIVIL_JUDGEMENT_DECLARE
        //===========================================================================================================================================
        String ICR_CIVIL_JUDGEMENT_DECLARE = "select\n" +
                "                  t1.report_id  as  report_id,\n" +
                "                  info.PF02ZD01 as civ_decl_type,\n" +
                "                  info.PF02ZQ01 as civ_decl_cont,\n" +
                "                  info.PF02ZR01 as civ_decl_dt,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  report_id \t\t\tas report_id,\n" +
                "                  SID \t\t\t\tas SID,\n" +
                "                  STATISTICS_DT \t\tas STATISTICS_DT,\n" +
                "                  PF02Z.PF02ZH     as data\n" +
                "                  from PCJPF02\n" +
                "                  )t1,unnest(t1.data) as info(PF02ZD01,PF02ZQ01,PF02ZR01) where t1.data is not null";


        createView(tableEnv, ICR_CIVIL_JUDGEMENT_DECLARE, "ICR_CIVIL_JUDGEMENT_DECLARE");
        //    tableEnv.sqlQuery("select * from ICR_CIVIL_JUDGEMENT_DECLARE").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_ENFORCEMENT_DECLARE
        //===========================================================================================================================================
        String ICR_ENFORCEMENT_DECLARE = "select\n" +
                "                  t1.report_id  as  report_id,\n" +
                "                  info.PF03ZD01 as enf_decl_type,\n" +
                "                  info.PF03ZQ01 as enf_decl_cont,\n" +
                "                  info.PF03ZR01 as enf_decl_dt,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  report_id \t\t\tas report_id,\n" +
                "                  SID \t\t\t\tas SID,\n" +
                "                  STATISTICS_DT \t\tas STATISTICS_DT,\n" +
                "                  PF03Z.PF03ZH        as data\n" +
                "                  from PCEPF03\n" +
                "                  )t1,unnest(t1.data) as info(PF03ZD01,PF03ZQ01,PF03ZR01) where t1.data is not null";


        createView(tableEnv, ICR_ENFORCEMENT_DECLARE, "ICR_ENFORCEMENT_DECLARE");
        //    tableEnv.sqlQuery("select * from ICR_ENFORCEMENT_DECLARE").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_ADMINPUNISHMENT_DECLARE
        //===========================================================================================================================================
        String ICR_ADMINPUNISHMENT_DECLARE = "select\n" +
                "                  t1.report_id  as  report_id,\n" +
                "                  info.PF04ZD01 as punish_decl_type,\n" +
                "                  info.PF04ZQ01 as punish_decl_cont,\n" +
                "                  info.PF04ZR01 as punish_decl_dt,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  report_id \t\t\tas report_id,\n" +
                "                  SID \t\t\t\tas SID,\n" +
                "                  STATISTICS_DT \t\tas STATISTICS_DT,\n" +
                "                  PF04Z.PF04ZH     as data\n" +
                "                  from PAPPF04\n" +
                "                  )t1,unnest(t1.data) as info(PF04ZD01,PF04ZQ01,PF04ZR01) where t1.data is not null";


        createView(tableEnv, ICR_ADMINPUNISHMENT_DECLARE, "ICR_ADMINPUNISHMENT_DECLARE");
        //    tableEnv.sqlQuery("select * from ICR_ADMINPUNISHMENT_DECLARE").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_HOUSINGFUND_DECLARE
        //===========================================================================================================================================
        String ICR_HOUSINGFUND_DECLARE = "select\n" +
                "                  t1.report_id  as  report_id,\n" +
                "                  info.PF05ZD01 as housing_decl_type,\n" +
                "                  info.PF05ZQ01 as housing_decl_cont,\n" +
                "                  info.PF05ZR01 as housing_decl_dt,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  report_id \t\t\tas report_id,\n" +
                "                  SID \t\t\t\tas SID,\n" +
                "                  STATISTICS_DT \t\tas STATISTICS_DT,\n" +
                "                  PF05Z.PF05ZH     as data\n" +
                "                  from PHFPF05\n" +
                "                  )t1,unnest(t1.data) as info(PF05ZD01,PF05ZQ01,PF05ZR01) where t1.data is not null";


        createView(tableEnv, ICR_HOUSINGFUND_DECLARE, "ICR_HOUSINGFUND_DECLARE");
        //    tableEnv.sqlQuery("select * from ICR_HOUSINGFUND_DECLARE").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_ALLOWANCE_DECLARE
        //===========================================================================================================================================
        String ICR_ALLOWANCE_DECLARE = "select\n" +
                "                  t1.report_id  as  report_id,\n" +
                "                  info.PF06ZD01 as allowance_decl_type,\n" +
                "                  info.PF06ZQ01 as allowance_decl_cont,\n" +
                "                  info.PF06ZR01 as allowance_decl_dt,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  report_id \t\t\tas report_id,\n" +
                "                  SID \t\t\t\tas SID,\n" +
                "                  STATISTICS_DT \t\tas STATISTICS_DT,\n" +
                "                  PF06Z.PF06ZH     as data\n" +
                "                  from PBSPF06\n" +
                "                  )t1,unnest(t1.data) as info(PF06ZD01,PF06ZQ01,PF06ZR01) where t1.data is not null";


        createView(tableEnv, ICR_ALLOWANCE_DECLARE, "ICR_ALLOWANCE_DECLARE");
        //    tableEnv.sqlQuery("select * from ICR_ALLOWANCE_DECLARE").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_QUALIFICATION_DECLARE
        //===========================================================================================================================================
        String ICR_QUALIFICATION_DECLARE = "select\n" +
                "                  t1.report_id  as  report_id,\n" +
                "                  info.PF07ZD01 as qual_decl_type,\n" +
                "                  info.PF07ZQ01 as qual_decl_cont,\n" +
                "                  info.PF07ZR01 as qual_decl_dt,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  report_id \t\t\tas report_id,\n" +
                "                  SID \t\t\t\tas SID,\n" +
                "                  STATISTICS_DT \t\tas STATISTICS_DT,\n" +
                "                  PF07Z.PF07ZH     as data\n" +
                "                  from PPQPF07\n" +
                "                  )t1,unnest(t1.data) as info(PF07ZD01,PF07ZQ01,PF07ZR01) where t1.data is not null";


        createView(tableEnv, ICR_QUALIFICATION_DECLARE, "ICR_QUALIFICATION_DECLARE");
        //    tableEnv.sqlQuery("select * from ICR_QUALIFICATION_DECLARE").toAppendStream[Row].print()
        //===========================================================================================================================================
        //                                                          todo ICR_OTHER_DECLARE_NUM
        //===========================================================================================================================================


        //===========================================================================================================================================
        //                                                          todo ICR_REWARD_DECLARE
        //===========================================================================================================================================
        String ICR_REWARD_DECLARE = " select\n" +
                "                  t1.report_id  as  report_id,\n" +
                "                  info.PF08ZD01 as rew_decl_type,\n" +
                "                  info.PF08ZQ01 as rew_decl_cont,\n" +
                "                  info.PF08ZR01 as rew_decl_dt,\n" +
                "                  t1.SID as SID,\n" +
                "                  t1.STATISTICS_DT as STATISTICS_DT\n" +
                "                  from(\n" +
                "                  select\n" +
                "                  report_id \t\t\tas report_id,\n" +
                "                  SID \t\t\t\tas SID,\n" +
                "                  STATISTICS_DT \t\tas STATISTICS_DT,\n" +
                "                  PF08Z.PF08ZH     as data\n" +
                "                  from PAHPF08\n" +
                "                  )t1,unnest(t1.data) as info(PF08ZD01,PF08ZQ01,PF08ZR01) where t1.data is not null";


        createView(tableEnv, ICR_REWARD_DECLARE, "ICR_REWARD_DECLARE");
        //===========================================================================================================================================
        //                                                          todo ICR_OTHER_DECLARE_NUM
        //===========================================================================================================================================

        String ICR_OTHER_DECLARE_NUM = "select\n" +
                "t1.report_id                            as report_id,\n" +
                "info.PG010D01                           as oth_type_cd,\n" +
                "info.PG010D02                           as oth_sign_cd,\n" +
                "cast(info.PG010S01 as bigint)           as oth_decl_num,\n" +
                "t1.STATISTICS_DT                        as STATISTICS_DT\n" +
                "from (\n" +
                "select\n" +
                "PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "POS.PG01         as data,\n" +
                "CURRENT_DATE                            as STATISTICS_DT\n" +
                "from ods_table\n" +
                ")t1,unnest(t1.data) as info(PG010D01,PG010D02,PG010S01,PG010H) where t1.data is not null";

        createView(tableEnv, ICR_OTHER_DECLARE_NUM, "ICR_OTHER_DECLARE_NUM");
        //===========================================================================================================================================
        //                                                          todo ICR_OTHER_DECLARE
        //===========================================================================================================================================
        String ICR_OTHER_DECLARE = "select\n" +
                "t2.report_id                            as report_id,\n" +
                "info2.PG010D03                          as oth_decl_type_cd,\n" +
                "info2.PG010Q01                          as oth_decl_cont,\n" +
                "info2.PG010R01                          as oth_decl_dt,\n" +
                "t2.SID                                  as SID,\n" +
                "t2.STATISTICS_DT                        as STATISTICS_DT\n" +
                "from(\n" +
                "select\n" +
                "t1.report_id                            as report_id,\n" +
                "info.PG010H                             as data2,\n" +
                "t1.SID                                  as SID,\n" +
                "t1.STATISTICS_DT                        as STATISTICS_DT\n" +
                "from (\n" +
                "select\n" +
                "PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n" +
                "POS.PG01         as data,\n" +
                "PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n" +
                "CURRENT_DATE                            as STATISTICS_DT\n" +
                "from ods_table\n" +
                ")t1,unnest(t1.data) as info(PG010D01,PG010D02,PG010S01,PG010H) where t1.data is not null\n" +
                ")t2,unnest(t2.data2) as info2(PG010D03,PG010Q01,PG010R01) where t2.data2 is not null";
        createView(tableEnv, ICR_OTHER_DECLARE, "ICR_OTHER_DECLARE");


        //===========================================================================================================================================
        //                                                          todo ICR_REPAYMENTDUTY_INFO
        //===========================================================================================================================================
        String ICR_REPAYMENTDUTY_INFO = "select\n" +
                "report_id as report_id,\n" +
                "'' as acct_num,\n" +
                "PD03A.PD03AD08 as repduty_iden_type,\n" +
                "PD03A.PD03AD01 as repduty_org_type,\n" +
                "PD03A.PD03AQ01 as repduty_org_id,\n" +
                "PD03A.PD03AD02 as repduty_busi_type,\n" +
                "PD03A.PD03AR01 as repduty_open_dt,\n" +
                "PD03A.PD03AR02 as repduty_due_dt,\n" +
                "PD03A.PD03AD03 as repduty_type_cd,\n" +
                "PD03A.PD03AQ02 as contruct_id,\n" +
                "cast(PD03A.PD03AJ01 as decimal(18,2)) as repduty_amt,\n" +
                "cast(PD03A.PD03AD04 as bigint) as repduty_currency_cd,\n" +
                "cast(PD03A.PD03AJ02 as decimal(18,2)) as repduty_bal,\n" +
                "PD03A.PD03AD05 as repduty_5class,\n" +
                "PD03A.PD03AD06 as repduty_acct_stat,\n" +
                "PD03A.PD03AD07 as repduty_repay_stat,\n" +
                "cast(PD03A.PD03AS01 as bigint) as repduty_overdue_mon,\n" +
                "PD03A.PD03AR03 as repduty_report_dt,\n" +
                "SID as SID,\n" +
                "STATISTICS_DT as STATISTICS_DT\n" +
                "from PCRPD03";
        createView(tableEnv, ICR_REPAYMENTDUTY_INFO, "ICR_REPAYMENTDUTY_INFO");


        //===========================================================================================================================================
        //                                                          todo ICR_REPAYMENTDUTY_DECL_NUM
        //===========================================================================================================================================
        String ICR_REPAYMENTDUTY_DECL_NUM = "select\n" +
                "report_id as report_id,\n" +
                "'' as acct_num,\n" +
                "cast(PD03Z.PD03ZS01 as bigint) as repduty_decl_num,\n" +
                "SID as SID,\n" +
                "STATISTICS_DT as STATISTICS_DT\n" +
                "from PCRPD03";
        createView(tableEnv, ICR_REPAYMENTDUTY_DECL_NUM, "ICR_REPAYMENTDUTY_DECL_NUM");
        //    tableEnv.sqlQuery("select * from ICR_REPAYMENTDUTY_DECL_NUM").toAppendStream[Row].print()

        //===========================================================================================================================================
        //                                                          todo ICR_REPAYMENTDUTY_DECL
        //===========================================================================================================================================
        String ICR_REPAYMENTDUTY_DECL = "select\n" +
                "t1.report_id as report_id,\n" +
                "'' as acct_num,\n" +
                "info.PD03ZD01 as repduty_decl_type,\n" +
                "info.PD03ZQ01 as repduty_decl_cont,\n" +
                "info.PD03ZR01 as repduty_decl_dt,\n" +
                "t1.SID as SID,\n" +
                "t1.STATISTICS_DT as STATISTICS_DT\n" +
                "from(\n" +
                "select\n" +
                "report_id as report_id,\n" +
                "PD03Z.PD03ZH as data,\n" +
                "SID as SID,\n" +
                "STATISTICS_DT as STATISTICS_DT\n" +
                "from PCRPD03\n" +
                ")t1,unnest(t1.data) as info(PD03ZD01,PD03ZQ01,PD03ZR01) where t1.data is not null";
        createView(tableEnv, ICR_REPAYMENTDUTY_DECL, "ICR_REPAYMENTDUTY_DECL");
        //    tableEnv.sqlQuery("select * from ICR_REPAYMENTDUTY_DECL").toAppendStream[Row].print()

        //todo 测试输出到kafka
//        String sink_table3 = " CREATE TABLE sink_table3 (\n" +
//                "                      a1 string,\n" +
//                "                      a2 string,\n" +
//                "                      a3 string,\n" +
//                "                      a4 string,\n" +
//                "                      a6 DATE\n" +
//                "                      )\n" +
//                "                      WITH (\n" +
//                "                        'connector' = 'kafka',\n" +
//                "                        'topic' = 'qinghuatest-011',\n" +
//                "                        'properties.group.id'='dev_flink',\n" +
//                "                        'properties.zookeeper.connect'='10.1.30.6:2181',\n" +
//                "                        'properties.bootstrap.servers' = '10.1.30.8:9092',\n" +
//                "                        'format' = 'json'\n" +
//                "                        )";
//
//
//        tableEnv.executeSql(sink_table3);
//        tableEnv.executeSql("insert into sink_table3 select * from ICR_CREDITSCORE");

        //===========================================================================================================================================
        //                                                          todo 开始测试特征sql
        //===========================================================================================================================================

        String TEMP_CREDITCARD_INFO = "" +
                "create view TEMP_CREDITCARD_INFO as\n" +
                "select \n" +
                "    a.report_id,\n" +
                "    a.acct_num,\n" +
                "    a.acct_type_cd,\n" +
                "    a.currency_cd,\n" +
                "    if(b.acct_stat_1m is not null,b.acct_stat_1m,c.acct_stat) as acct_stat_final,\n" +
                "    a.credit_amt,\n" +
                "    a.credit_amt_share,\n" +
                "    a.open_dt,\n" +
                "    a.acct_org_id,\n" +
                "    if(b.credit_6mon_avg is not null,b.credit_6mon_avg,b.overdraft_6mon_avg) as bal_6mon_avg,\n" +
                "    if(b.max_credit_used is not null,b.max_credit_used,b.max_overdraft) as max_bal_used, \n" +
                "    b.overdraft_prin_180Amt,\n" +
                "    if(c.loan_bal is not null,c.loan_bal,b.loan_bal_1m) as loan_bal_final,                \n" +
                "    b.overdue_period,\n" +
                "    b.overdue_amt,    \n" +
                "    c.repay_stat,\n" +
                "    d.report_no as report_no,\n" +
                "    substr(d.report_tm,1,10) as report_tm\n" +
                "from \n" +
                "(SELECT \n" +
                "    report_id    \n" +
                "    ,report_no as report_no\n" +
                "    ,report_tm AS report_tm\n" +
                "    ,cust_name\n" +
                "    ,query_iden_cd\n" +
                "    ,query_iden_id\n" +
                "    ,query_org_id\n" +
                "    ,query_reason_cd as query_reason_cd\n" +
                "    ,STATISTICS_DT\n" +
                "FROM \n" +
                "    ICR_QUERYREQ) d\n" +
                "left join\n" +
                "(select * from ICR_LOAN_INFO  where acct_type_cd in ('R2','R3') and open_dt is distinct from '1900-09-09') a\n" +
                "on  d.report_id = a.report_id\n" +
                "left join \n" +
                "(select * from ICR_LOAN_1MONTH) b\n" +
                "on d.report_id = b.report_id and a.acct_num=b.acct_num\n" +
                "left join \n" +
                "(select * from ICR_LOAN_LATEST) c\n" +
                "on d.report_id = c.report_id and a.acct_num=c.acct_num";

//        createView(tableEnv, TEMP_CREDITCARD_INFO, "TEMP_CREDITCARD_INFO");
        tableEnv.executeSql(TEMP_CREDITCARD_INFO);
        String print_t = "create table print_t(\n" +
                "a int\n" +
                ")with(\n" +
                "'connector'='print'\n" +
                ")";
        tableEnv.executeSql(print_t);

        //tableEnv.executeSql("insert into print_t select flink_months_between_str(from_unixtime(unix_timestamp('20200811','yyyyMMdd'),'yyyy-MM-dd'),open_dt) from TEMP_CREDITCARD_INFO");

        Table table = tableEnv.sqlQuery("select flink_months_between_str(from_unixtime(unix_timestamp('20200811','yyyyMMdd'),'yyyy-MM-dd'),open_dt) from TEMP_CREDITCARD_INFO");


        tableEnv.toRetractStream(table, Row.class).iterate().print();
        tableEnv.execute("aa");
        env.execute();
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
