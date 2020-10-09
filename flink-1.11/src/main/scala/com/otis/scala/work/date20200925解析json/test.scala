package com.otis.scala.work.date20200925解析json

import com.otis.work.date20200925解析json.Json2StringFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row


object test {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)
    val ods_table =
      """
        |create table ods_table(
        |PRH ROW(PA01 ROW(
        |                  PA01B ROW(PA01BQ01 string,PA01BD01 string,PA01BI01 string,PA01BI02 string,PA01BD02 string),
        |                  PA01A ROW(PA01AI01 string,PA01AR01 string),
        |                  PA01C ROW(PA01CS01 string,PA01CH ARRAY<ROW(PA01CD01 string,PA01CI01 string)>),
        |                  PA01D ROW(PA01DQ01 string,PA01DQ02 string,PA01DR01 string,PA01DR02 string),
        |                  PA01E ROW(PA01ES01 STRING)
        |                 )),
        |PIM ROW(PB01 ROW(
        |                  PB01A ROW(PB01AD01 STRING,PB01AR01 STRING,PB01AD02 STRING,PB01AD03 STRING,PB01AD04 STRING,PB01AQ01 STRING,PB01AQ02 STRING,PB01AD05 STRING,PB01AQ03 STRING),
        |                  PB01B ROW(PB01BS01 STRING,PB01BH ARRAY<ROW(PB01BQ01 STRING,PB01BR01 STRING)>)
        |        )),
        |PMM ROW(PB02 ROW(PB020D01 STRING,PB020Q01 STRING,PB020D02 STRING,PB020I01 STRING,PB020Q02 STRING,PB020Q03 STRING)),
        |PRM ROW(PB03 ARRAY<ROW(PB030D01 STRING,PB030Q01 STRING,PB030Q02 STRING,PB030R01 STRING)>),
        |POM ROW(PB04 ARRAY<ROW(PB040D01 STRING,PB040Q01 STRING,PB040D02 STRING,PB040D03 STRING,PB040Q02 STRING,PB040Q03 STRING,PB040D04 STRING,PB040D05 STRING,PB040D06 STRING,PB040R01 STRING,PB040R02 STRING)>),
        |PSM ROW(PC01 ROW(PC010Q01 STRING,PC010Q02 STRING,PC010S01 STRING,PC010D01 STRING)),
        |PCO ROW(PC02 ROW(
        |                  PC02A ROW(PC02AS01 STRING,PC02AS02 STRING,PC02AH ARRAY<ROW(PC02AD01 STRING,PC02AD02 STRING,PC02AS03 STRING,PC02AR01 STRING)>),
        |                  PC02B ROW(PC02BS01 STRING,PC02BJ01 STRING,PC02BS02 STRING,PC02BH ARRAY<ROW(PC02BD01 STRING,PC02BS03 STRING,PC02BJ02 STRING)>),
        |                  PC02C ROW(PC02CS01 STRING,PC02CJ01 STRING),
        |                  PC02D ROW(PC02DS01 STRING,PC02DH ARRAY<ROW(PC02DD01 STRING,PC02DS02 STRING,PC02DS03 STRING,PC02DJ01 STRING,PC02DS04 STRING)>),
        |                  PC02E ROW(PC02ES01 STRING,PC02ES02 STRING,PC02EJ01 STRING,PC02EJ02 STRING,PC02EJ03 STRING),
        |                  PC02F ROW(PC02FS01 STRING,PC02FS02 STRING,PC02FJ01 STRING,PC02FJ02 STRING,PC02FJ03 STRING),
        |                  PC02G ROW(PC02GS01 STRING,PC02GS02 STRING,PC02GJ01 STRING,PC02GJ02 STRING,PC02GJ03 STRING),
        |                  PC02H ROW(PC02HS01 STRING,PC02HS02 STRING,PC02HJ01 STRING,PC02HJ02 STRING,PC02HJ03 STRING,PC02HJ04 STRING,PC02HJ05 STRING),
        |                  PC02I ROW(PC02IS01 STRING,PC02IS02 STRING,PC02IJ01 STRING,PC02IJ02 STRING,PC02IJ03 STRING,PC02IJ04 STRING,PC02IJ05 STRING),
        |                  PC02K ROW(PC02KS01 STRING,PC02KH ARRAY<ROW(PC02KD01 STRING,PC02KD02 STRING,PC02KS02 STRING,PC02KJ01 STRING,PC02KJ02 STRING)>))
        |      ),
        |PNO ROW(PC03 ROW(PC030S01 STRING,PC030H ARRAY<ROW(PC030D01 STRING,PC030S02 STRING,PC030J01 STRING)>)),
        |PPO ROW(PC04 ROW(PC040S01 STRING,PC040H ARRAY<ROW(PC040D01 STRING,PC040S02 STRING,PC040J01 STRING)>)),
        |PQO ROW(PC05 ROW(
        |                    PC05A ROW(PC05AR01 STRING,PC05AD01 STRING,PC05AI01 STRING,PC05AQ01 STRING),
        |                    PC05B ROW(PC05BS01 STRING,PC05BS02 STRING,PC05BS03 STRING,PC05BS04 STRING,PC05BS05 STRING,PC05BS06 STRING,PC05BS07 STRING,PC05BS08 STRING)
        |    )),
        |PDA ROW(PD01 ARRAY<ROW(
        |                    PD01A ROW(PD01AI01 STRING,PD01AD01 STRING,PD01AD02 STRING,PD01AI02 STRING,PD01AI03 STRING,PD01AI04 STRING,PD01AD03 STRING,PD01AR01 STRING,PD01AD04 STRING,PD01AJ01 STRING,PD01AJ02 STRING,PD01AJ03 STRING,PD01AR02 STRING,PD01AD05 STRING,PD01AD06 STRING,PD01AS01 STRING,PD01AD07 STRING,PD01AD08 STRING,PD01AD09 STRING,PD01AD10 STRING),
        |                    PD01B ROW(PD01BD01 STRING,PD01BR01 STRING,PD01BR04 STRING,PD01BJ01 STRING,PD01BR02 STRING,PD01BJ02 STRING,PD01BD03 STRING,PD01BD04 STRING,PD01BR03 STRING),
        |                    PD01C ROW(PD01CR01 STRING,PD01CD01 STRING,PD01CJ01 STRING,PD01CJ02 STRING,PD01CJ03 STRING,PD01CD02 STRING,PD01CS01 STRING,PD01CR02 STRING,PD01CJ04 STRING,PD01CJ05 STRING,PD01CR03 STRING,PD01CS02 STRING,PD01CJ06 STRING,PD01CJ07 STRING,PD01CJ08 STRING,PD01CJ09 STRING,PD01CJ10 STRING,PD01CJ11 STRING,PD01CJ12 STRING,PD01CJ13 STRING,PD01CJ14 STRING,PD01CJ15 STRING,PD01CR04 STRING),
        |                    PD01D ROW(PD01DR01 STRING,PD01DR02 STRING,PD01DH ARRAY<ROW(PD01DR03 STRING,PD01DD01 STRING)>),
        |                    PD01E ROW(PD01ER01 STRING,PD01ER02 STRING,PD01ES01 STRING,PD01EH ARRAY<ROW(PD01ER03 STRING,PD01ED01 STRING,PD01EJ01 STRING)>),
        |                    PD01F ROW(PD01FS01 STRING,PD01FH ARRAY<ROW(PD01FD01 STRING,PD01FR01 STRING,PD01FS02 STRING,PD01FJ01 STRING,PD01FQ01 STRING)>),
        |                    PD01G ROW(PD01GS01 STRING,PD01GH ARRAY<ROW(PD01GR01 STRING,PD01GD01 STRING)>),
        |                    PD01H ROW(PD01HS01 STRING,PD01HH ARRAY<ROW(PD01HJ01 STRING,PD01HR01 STRING,PD01HR02 STRING,PD01HJ02 STRING)>),
        |                    PD01Z ROW(PD01ZS01 STRING,PD01ZH ARRAY<ROW(PD01ZD01 STRING,PD01ZQ01 STRING,PD01ZR01 STRING)>)
        |      )>),
        |PCA ROW(PD02 ARRAY<ROW(
        |                        PD02A ROW(PD02AI01 STRING,PD02AD01 STRING,PD02AI02 STRING,PD02AI03 STRING,PD02AD02 STRING,PD02AJ01 STRING,PD02AD03 STRING,PD02AR01 STRING,PD02AR02 STRING,PD02AD04 STRING,PD02AJ04 STRING,PD02AJ03 STRING,PD02AI04 STRING),
        |                        PD02Z ROW(PD02ZS01 STRING,PD02ZH ARRAY<ROW(PD02ZD01 STRING,PD02ZQ01 STRING,PD02ZR01 STRING)>)
        |                        )>),
        |PCR ROW(PD03 ARRAY<ROW(
        |                        PD03A ROW(PD03AD08 STRING,PD03AD01 STRING,PD03AQ01 STRING,PD03AD02 STRING,PD03AR01 STRING,PD03AR02 STRING,PD03AD03 STRING,PD03AQ02 STRING,PD03AJ01 STRING,PD03AD04 STRING,PD03AJ02 STRING,PD03AD05 STRING,PD03AD06 STRING,PD03AD07 STRING,PD03AS01 STRING,PD03AR03 STRING),
        |                        PD03Z ROW(PD03ZS01 STRING,PD03ZH ARRAY<ROW(PD03ZD01 STRING)>)
        |                      )>),
        |PND ROW(PE01 ARRAY<ROW(
        |                        PE01A ROW(PE01AD01 STRING,PE01AQ01 STRING,PE01AD02 STRING,PE01AR01 STRING,PE01AD03 STRING,PE01AJ01 STRING,PE01AR02 STRING,PE01AQ02 STRING),
        |                        PE01Z ROW(PE01ZS01 STRING,PE01ZH ARRAY<ROW(PE01ZD01 STRING,PE01ZQ01 STRING,PE01ZR01 STRING)>)
        |                )>),
        |POT ROW(PF01 ARRAY<ROW(
        |                        PF01A ROW(PF01AQ01 STRING,PF01AJ01 STRING,PF01AR01 STRING),
        |                        PF01Z ROW(PF01ZS01 STRING,PF01ZH ARRAY<ROW(PF01ZD01 STRING,PF01ZQ01 STRING,PF01ZR01 STRING)>))
        |                >),
        |PCJ ROW(PF02 ARRAY<ROW(
        |                        PF02A ROW(PF02AQ01 STRING,PF02AQ02 STRING,PF02AR01 STRING,PF02AD01 STRING,PF02AQ03 STRING,PF02AR02 STRING,PF02AQ04 STRING,PF02AJ01 STRING),
        |                        PF02Z ROW(PF02ZS01 STRING,PF02ZH ARRAY<ROW(PF02ZD01 STRING,PF02ZQ01 STRING,PF02ZR01 STRING)>)
        |                )>),
        |PCE ROW(PF03 ARRAY<ROW(
        |                        PF03A ROW(PF03AQ01 STRING,PF03AQ02 STRING,PF03AR01 STRING,PF03AD01 STRING,PF03AQ03 STRING,PF03AR02 STRING,PF03AQ04 STRING,PF03AJ01 STRING,PF03AQ05 STRING,PF03AJ02 STRING),
        |                        PF03Z ROW(PF03ZS01 STRING,PF03ZH ARRAY<ROW(PF03ZD01 STRING,PF03ZQ01 STRING,PF03ZR01 STRING)>)
        |                )>),
        |PAP ROW(PF04 ARRAY<ROW(
        |                        PF04A ROW(PF04AQ01 STRING,PF04AQ02 STRING,PF04AJ01 STRING,PF04AR01 STRING,PF04AR02 STRING,PF04AQ03 STRING),
        |                        PF04Z ROW(PF04ZS01 STRING,PF04ZH ARRAY<ROW(PF04ZD01 STRING,PF04ZQ01 STRING,PF04ZR01 STRING)>)
        |                )>),
        |PHF ROW(PF05 ARRAY<ROW(
        |                        PF05A ROW(PF05AQ01 STRING,PF05AR01 STRING,PF05AD01 STRING,PF05AR02 STRING,PF05AR03 STRING,PF05AQ02 STRING,PF05AQ03 STRING,PF05AJ01 STRING,PF05AQ04 STRING,PF05AR04 STRING),
        |                        PF05Z ROW(PF05ZS01 STRING,PF05ZH ARRAY<ROW(PF05ZD01 STRING,PF05ZQ01 STRING,PF05ZR01 STRING)>)
        |                )>),
        |PBS ROW(PF06 ARRAY<ROW(
        |                        PF06A ROW(PF06AD01 STRING,PF06AQ01 STRING,PF06AQ02 STRING,PF06AQ03 STRING,PF06AR01 STRING,PF06AR02 STRING,PF06AR03 STRING),
        |                        PF06Z ROW(PF06ZS01 STRING,PF06ZH ARRAY<ROW(PF06ZD01 STRING,PF06ZQ01 STRING,PF06ZR01 STRING)>)
        |                )>),
        |PPQ ROW(PF07 ARRAY<ROW(
        |                        PF07A ROW(PF07AQ01 STRING,PF07AQ02 STRING,PF07AD01 STRING,PF07AD02 STRING,PF07AR01 STRING,PF07AR02 STRING,PF07AR03 STRING),
        |                        PF07Z ROW(PF07ZS01 STRING,PF07ZH ARRAY<ROW(PF07ZD01 STRING,PF07ZQ01 STRING,PF07ZR01 STRING)>)
        |                )>),
        |PAH ROW(PF08 ARRAY<ROW(
        |                        PF08A ROW(PF08AQ01 STRING,PF08AQ02 STRING,PF08AR01 STRING,PF08AR02 STRING),
        |                        PF08Z ROW(PF08ZS01 STRING,PF08ZH ARRAY<ROW(PF08ZD01 STRING,PF08ZQ01 STRING,PF08ZR01 STRING)>)
        |                )>),
        |POS ROW(PG01 ARRAY<ROW(PG010D01 STRING,PG010D02 STRING)>),
        |POQ ROW(PH01 ARRAY<ROW(PH010R01 STRING,PH010D01 STRING,PH010Q02 STRING,PH010Q03 STRING)>)
        |)WITH(
        |'connector' = 'kafka',
        |'topic' = 'odsTable',
        |'properties.bootstrap.servers' = '10.1.30.8:9092',
        |'properties.group.id' = 'topic.group1',
        |'format' = 'json',
        |'scan.startup.mode' = 'earliest-offset'
        |)
        |""".stripMargin
    //    'connector' = 'filesystem',
    //    'path' = 'file:///D:\peoject\Official-FlinkStudy\flink-1.11\src\main\java\com\otis\work\date20200925解析json\test.json',
    //    'format' = 'json'
    tableEnv.executeSql(ods_table)
    //===========================================================================================================================================
    //                                                          todo ICR_QUERYREQ
    //===========================================================================================================================================

    val ICR_QUERYREQ_table = tableEnv.sqlQuery(
      """
        |select
        |PRH.PA01.PA01A.PA01AI01 as report_id,
        |PRH.PA01.PA01A.PA01AI01 as report_no,
        |PRH.PA01.PA01A.PA01AR01 as report_tm,
        |PRH.PA01.PA01B.PA01BQ01 as cust_name,
        |PRH.PA01.PA01B.PA01BD01 as query_iden_cd,
        |PRH.PA01.PA01B.PA01BI01 as query_iden_id,
        |PRH.PA01.PA01B.PA01BI02 as query_org_id,
        |PRH.PA01.PA01B.PA01BD02 as query_reason_cd,
        |'2020-09-27'            as STATISTICS_DT
        |from ods_table
        |""".stripMargin)

    //    ICR_QUERYREQ_table.toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_OTHER_IDEN_NUM
    //===========================================================================================================================================
    val ICR_OTHER_IDEN_NUM_table: Table = tableEnv.sqlQuery(
      """
        |select
        |PRH.PA01.PA01A.PA01AI01 as report_id,
        |PRH.PA01.PA01C.PA01CS01 as iden_type_num,
        |'2020-09-27'            as STATISTICS_DT
        |from ods_table
        |""".stripMargin)
    //    ICR_OTHER_IDEN_NUM_table.toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_OTHER_IDEN
    //===========================================================================================================================================

    var ICR_OTHER_IDEN: Table = null
    ICR_OTHER_IDEN = tableEnv.sqlQuery(
      """
        |select
        |report_id as report_id,
        |t2.a      as iden_cd,
        |t2.b      as iden_id,
        |SID		  as SID,
        |STATISTICS_DT as STATISTICS_DT
        |from
        |(select
        |PRH.PA01.PA01A.PA01AI01 as SID,
        |PRH.PA01.PA01A.PA01AI01 as report_id,
        |PRH.PA01.PA01C.PA01CH as ok,
        |'2020-09-27'            as STATISTICS_DT
        |from ods_table)t1,
        |unnest(t1.ok) as t2(a,b)
        |""".stripMargin)
    tableEnv.createTemporaryView("ICR_OTHER_IDEN", ICR_OTHER_IDEN)
    //    tableEnv.sqlQuery(
    //      """
    //        |select iden_cd from ICR_OTHER_IDEN
    //        |""".stripMargin).toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_FRAUD
    //===========================================================================================================================================

    val ICR_FRAUD_table = tableEnv.sqlQuery(
      """
        |select
        |PRH.PA01.PA01A.PA01AI01 as report_id,
        |PRH.PA01.PA01D.PA01DQ01 as fraud_cd,
        |PRH.PA01.PA01D.PA01DQ02 as fraud_tel,
        |PRH.PA01.PA01D.PA01DR01 as fraud_start_dt,
        |PRH.PA01.PA01D.PA01DR02 as fraud_end_dt,
        |cast(PRH.PA01.PA01E.PA01ES01 as bigint) as objection_num,
        |'2020-09-27'            as STATISTICS_DT
        |from ods_table
        |""".stripMargin)
    tableEnv.createTemporaryView("ICR_FRAUD", ICR_FRAUD_table)
    //tableEnv.sqlQuery("""select * from ICR_FRAUD""").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_IDENTITY
    //===========================================================================================================================================

    val ICR_IDENTITY = tableEnv.sqlQuery(
      """
        |select
        |PRH.PA01.PA01A.PA01AI01                 as report_id,
        |PIM.PB01.PB01A.PB01AD01                 as gender_cd,
        |PIM.PB01.PB01A.PB01AR01                 as birth_date,
        |PIM.PB01.PB01A.PB01AD02                 as edu_level_cd,
        |PIM.PB01.PB01A.PB01AD03                 as edu_degree_cd,
        |PIM.PB01.PB01A.PB01AD04                 as employment_cd,
        |PIM.PB01.PB01A.PB01AQ01                 as email,
        |PIM.PB01.PB01A.PB01AQ02                 as comm_addr,
        |PIM.PB01.PB01A.PB01AD05                 as nationality,
        |PIM.PB01.PB01A.PB01AQ03                 as reg_addr,
        |cast(PIM.PB01.PB01B.PB01BS01 as bigint) as tel_cnt,
        |'2020-09-27'                            as STATISTICS_DT
        |from ods_table
        |""".stripMargin)
    tableEnv.createTemporaryView("ICR_IDENTITY", ICR_IDENTITY)
    //    tableEnv.sqlQuery("select * from ICR_IDENTITY").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_TEL
    //===========================================================================================================================================
    val ICR_TEL = tableEnv.sqlQuery(
      """
        |select
        |t1.report_id                            as report_id,
        |info.PB01BQ01                           as tel_num,
        |info.PB01BR01                           as update_dt,
        |t1.SID                                  as SID,
        |t1.STATISTICS_DT                        as STATISTICS_DT
        |from(
        |SELECT
        |PRH.PA01.PA01A.PA01AI01                 as report_id,
        |PIM.PB01.PB01B.PB01BH                   as data,
        |PRH.PA01.PA01A.PA01AI01                 as SID,
        |'2020-09-27'                            as STATISTICS_DT
        |FROM ods_table)t1,unnest(t1.data) as info(PB01BQ01,PB01BR01)
        |""".stripMargin)
    tableEnv.createTemporaryView("ICR_TEL", ICR_TEL)
    tableEnv.sqlQuery("select * from ICR_TEL").toAppendStream[Row].print()
    //    val sink_table4 =
    //      """
    //        |CREATE TABLE sink_table4 (
    //        |    a1 string,
    //        |    a2 string,
    //        |    a3 string,
    //        |    a4 string,
    //        |    a5 string
    //        |    )
    //        |    WITH (
    //        |      'connector' = 'kafka',
    //        |      'topic' = 'qinghuatest-001',
    //        |      'properties.group.id'='dev_flink',
    //        |      'properties.zookeeper.connect'='10.1.30.6:2181',
    //        |      'properties.bootstrap.servers' = '10.1.30.8:9092',
    //        |      'format' = 'json',
    //        |      'scan.startup.mode' = 'latest-offset'
    //        |      )
    //        |""".stripMargin
    //    tableEnv.executeSql(sink_table4)
    //    print(tableEnv.explainSql("insert into sink_table4 select * from ICR_TEL"))
    //    tableEnv.sqlUpdate("insert into sink_table4 select * from ICR_TEL")
    //===========================================================================================================================================
    //                                                          todo ICR_SPOUSE
    //===========================================================================================================================================
    val ICR_SPOUSE = tableEnv.sqlQuery(
      """
        |SELECT
        |PRH.PA01.PA01A.PA01AI01                 as report_id,
        |PMM.PB02.PB020D01                       as marital_stat_cd,
        |PMM.PB02.PB020Q01                       as spo_name,
        |PMM.PB02.PB020D02                       as spo_iden_cd,
        |PMM.PB02.PB020I01                       as spo_iden_id,
        |PMM.PB02.PB020Q02                       as spo_unit,
        |PMM.PB02.PB020Q03                       as spo_tel_num,
        |'2020-09-27'                            as STATISTICS_DT
        |FROM ods_table
        |""".stripMargin)
    tableEnv.createTemporaryView("ICR_SPOUSE", ICR_SPOUSE)
    //    tableEnv.sqlQuery("select * from ICR_SPOUSE").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_RESIDENCE
    //===========================================================================================================================================

    val ICR_RESIDENCE = tableEnv.sqlQuery(
      """
        |select
        |t1.report_id                            as report_id,
        |info.PB030D01                           as res_cd,
        |info.PB030Q01                           as res_addr,
        |info.PB030Q02                           as res_tel,
        |info.PB030R01                           as res_update_dt,
        |t1.SID                                  as SID,
        |t1.STATISTICS_DT                        as STATISTICS_DT
        |from
        |(
        |select
        |PRH.PA01.PA01A.PA01AI01                 as report_id,
        |PRM.PB03                                as data,
        |PRH.PA01.PA01A.PA01AI01                 as SID,
        |'2020-09-27'                            as STATISTICS_DT
        |from ods_table
        |)t1,unnest(t1.data) as info(PB030D01,PB030Q01,PB030Q02,PB030R01)
        |""".stripMargin)
    tableEnv.createTemporaryView("ICR_RESIDENCE", ICR_RESIDENCE)
    //    tableEnv.sqlQuery("select * from ICR_RESIDENCE").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_PROFESSION
    //===========================================================================================================================================
    val ICR_PROFESSION = tableEnv.sqlQuery(
      """
        |select
        |t1.report_id                            as report_id,
        |info.PB040D01                           as work_situation,
        |info.PB040Q01                           as work_unit,
        |info.PB040D02                           as unit_property_cd,
        |info.PB040D03                           as industry_cd,
        |info.PB040Q02                           as unit_addr,
        |info.PB040Q03                           as unit_tel,
        |info.PB040D04                           as occupation_cd,
        |info.PB040D05                           as position_cd,
        |info.PB040D06                           as title_cd,
        |info.PB040R01                           as int_year,
        |info.PB040R02                           as pro_update_date,
        |t1.SID                                  as SID,
        |t1.STATISTICS_DT                        as STATISTICS_DT
        |from
        |(
        |select
        |PRH.PA01.PA01A.PA01AI01                 as report_id,
        |POM.PB04                                as data,
        |PRH.PA01.PA01A.PA01AI01                 as SID,
        |'2020-09-27'                            as STATISTICS_DT
        |from ods_table
        |)t1,unnest(t1.data) as info(PB040D01,PB040Q01,PB040D02,PB040D03,PB040Q02,PB040Q03,PB040D04,PB040D05,PB040D06,PB040R01,PB040R02)
        |""".stripMargin)
    tableEnv.createTemporaryView("ICR_PROFESSION", ICR_PROFESSION)
    //    tableEnv.sqlQuery("select * from ICR_PROFESSION").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_CREDITSCORE
    //===========================================================================================================================================
    val ICR_CREDITSCORE = tableEnv.sqlQuery(
      """
        |select
        |PRH.PA01.PA01A.PA01AI01                 as report_id,
        |PSM.PC01.PC010Q01                       as score,
        |PSM.PC01.PC010Q02                       as score_level,
        |PSM.PC01.PC010S01                       as score_desc_num,
        |'2020-09-27'                            as STATISTICS_DT
        |from ods_table
        |""".stripMargin)
    tableEnv.createTemporaryView("ICR_CREDITSCORE", ICR_CREDITSCORE)
    //    tableEnv.sqlQuery("select * from ICR_CREDITSCORE").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_SCORE_DESC
    //===========================================================================================================================================
    val ICR_SCORE_DESC =
    """
      |select
      |PRH.PA01.PA01A.PA01AI01                 as report_id,
      |PSM.PC01.PC010D01                       as score_cd,
      |PRH.PA01.PA01A.PA01AI01                 as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table""".stripMargin
    createView(tableEnv, ICR_SCORE_DESC, "ICR_SCORE_DESC")
    //    tableEnv.sqlQuery("select * from ICR_SCORE_DESC").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_CREDIT_CUE_NUM
    //===========================================================================================================================================
    val ICR_CREDIT_CUE_NUM =
    """
      |select
      |PRH.PA01.PA01A.PA01AI01                 as report_id,
      |PCO.PC02.PC02A.PC02AS01                 as acct_total_cnt,
      |PCO.PC02.PC02A.PC02AS02                 as busi_type_num,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |""".stripMargin
    createView(tableEnv, ICR_CREDIT_CUE_NUM, "ICR_CREDIT_CUE_NUM")
    //    tableEnv.sqlQuery("select * from ICR_CREDIT_CUE_NUM").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_CREDIT_CUE
    //===========================================================================================================================================
    val ICR_CREDIT_CUE =
    """
      |select
      |t1.report_id                            as report_id,
      |info.PC02AD01                           as busi_type_cd,
      |info.PC02AD02                           as busi_kind_cd,
      |info.PC02AS03                           as acct_cnt,
      |info.PC02AR01                           as first_mon,
      |t1.SID                                  as SID,
      |t1.STATISTICS_DT                        as STATISTICS_DT
      |from
      |(select
      |PRH.PA01.PA01A.PA01AI01                 as report_id,
      |PCO.PC02.PC02A.PC02AH                   as data,
      |PRH.PA01.PA01A.PA01AI01                 as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table)t1,unnest(t1.data) as info(PC02AD01,PC02AD02,PC02AS03,PC02AR01)
      |""".stripMargin
    createView(tableEnv, ICR_CREDIT_CUE, "ICR_CREDIT_CUE")
    //    tableEnv.sqlQuery("select * from ICR_CREDIT_CUE").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_RECOUPED_NUM
    //===========================================================================================================================================
    val ICR_RECOUPED_NUM =
    """
      |select
      |PRH.PA01.PA01A.PA01AI01                 as report_id,
      |PCO.PC02.PC02B.PC02BS01                 as rec_total_cnt,
      |PCO.PC02.PC02B.PC02BJ01                 as rec_total_bal,
      |PCO.PC02.PC02B.PC02BS02                 as rec_type_num,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |""".stripMargin
    createView(tableEnv, ICR_RECOUPED_NUM, "ICR_RECOUPED_NUM")
    //    tableEnv.sqlQuery("select * from ICR_RECOUPED_NUM").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_RECOUPED_INFO
    //===========================================================================================================================================
    val ICR_RECOUPED_INFO =
    """
      |select
      |t1.report_id                            as report_id,
      |info.PC02BD01                           as rec_type_cd,
      |cast(info.PC02BS03 as bigint)           as rec_acct_cnt,
      |cast(info.PC02BJ02 as decimal(18,2))    as rec_bal,
      |t1.SID                                  as SID,
      |t1.STATISTICS_DT                        as STATISTICS_DT
      |from (
      |select
      |PRH.PA01.PA01A.PA01AI01                 as report_id,
      |PCO.PC02.PC02B.PC02BH                   as data,
      |PRH.PA01.PA01A.PA01AI01                 as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PC02BD01,PC02BS03,PC02BJ02)
      |""".stripMargin
    createView(tableEnv, ICR_RECOUPED_INFO, "ICR_RECOUPED_INFO")
    //    tableEnv.sqlQuery("select * from ICR_RECOUPED_INFO").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_DEADACCOUNT
    //===========================================================================================================================================
    val ICR_DEADACCOUNT =
    """
      |select
      |PRH.PA01.PA01A.PA01AI01                         as report_id,
      |cast(PCO.PC02.PC02C.PC02CS01 as bigint)         as deadacct_cnt,
      |cast(PCO.PC02.PC02C.PC02CJ01 as decimal(18,2))  as deadacct_bal,
      |'2020-09-27'                                    as STATISTICS_DT
      |from ods_table
      |""".stripMargin
    createView(tableEnv, ICR_DEADACCOUNT, "ICR_DEADACCOUNT")
    //    tableEnv.sqlQuery("select * from ICR_DEADACCOUNT").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_OVERDUE_NUM
    //===========================================================================================================================================
    val ICR_OVERDUE_NUM =
    """
      |select
      |PRH.PA01.PA01A.PA01AI01                 as report_id,
      |cast(PCO.PC02D.PC02DS01 as bigint)      as ove_type_num,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |""".stripMargin
    createView(tableEnv, ICR_OVERDUE_NUM, "ICR_OVERDUE_NUM")
    //    tableEnv.sqlQuery("select * from ICR_OVERDUE_NUM").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_OVERDUE
    //===========================================================================================================================================
    val ICR_OVERDUE =
    """
      |select
      |t1.report_id                            as report_id,
      |info.PC02DD01                           as ove_type_cd,
      |info.PC02DS02                           as ove_acct_cnt,
      |info.PC02DS03                           as ove_mon_num,
      |info.PC02DJ01                           as ove_bal,
      |info.PC02DS04                           as ove_mon,
      |t1.SID                                  as SID,
      |t1.STATISTICS_DT                        as STATISTICS_DT
      |from(
      |select
      |PRH.PA01.PA01A.PA01AI01                 as report_id,
      |PCO.PC02.PC02D.PC02DH                   as data,
      |PRH.PA01.PA01A.PA01AI01                 as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table)t1,unnest(t1.data) as info(PC02DD01,PC02DS02,PC02DS03,PC02DJ01,PC02DS04)
      |""".stripMargin
    createView(tableEnv, ICR_OVERDUE, "ICR_OVERDUE")
    //    tableEnv.sqlQuery("select * from ICR_OVERDUE").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_ONEOFF
    //===========================================================================================================================================
    val ICR_ONEOFF =
    """
      |    select
      |    PRH.PA01.PA01A.PA01AI01                 as report_id,
      |    cast(PCO.PC02.PC02E.PC02ES01 as bigint)                 as oneoff_org_num,
      |    cast(PCO.PC02.PC02E.PC02ES02 as bigint)                 as oneoff_acct_cnt,
      |    cast(PCO.PC02.PC02E.PC02EJ01 as decimal(18,2))                 as oneoff_credit_amt,
      |    cast(PCO.PC02.PC02E.PC02EJ02 as decimal(18,2))                 as oneoff_bal,
      |    cast(PCO.PC02.PC02E.PC02EJ03 as decimal(18,2))                 as oneoff_6mon_avg,
      |    '2020-09-27'                            as STATISTICS_DT
      |      from ods_table
      |""".stripMargin
    createView(tableEnv, ICR_ONEOFF, "ICR_ONEOFF")
    //    tableEnv.sqlQuery("select * from ICR_ONEOFF").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_REVOLVING_ACCT
    //===========================================================================================================================================
    val ICR_REVOLVING_ACCT =
    """
      |    select
      |    PRH.PA01.PA01A.PA01AI01                 as report_id,
      |    cast(PCO.PC02.PC02F.PC02FS01 as bigint)                 as revacct_org_num,
      |    cast(PCO.PC02.PC02F.PC02FS02 as bigint)                 as revacc_acct_cnt,
      |    cast(PCO.PC02.PC02F.PC02FJ01 as decimal(18,2))                 as revacct_credit_amt,
      |    cast(PCO.PC02.PC02F.PC02FJ02 as decimal(18,2))                 as revacct_bal,
      |    cast(PCO.PC02.PC02F.PC02FJ03 as decimal(18,2))                 as revacct_6mon_avg,
      |    '2020-09-27'                            as STATISTICS_DT
      |      from ods_table
      |""".stripMargin
    createView(tableEnv, ICR_REVOLVING_ACCT, "ICR_REVOLVING_ACCT")
    //    tableEnv.sqlQuery("select * from ICR_REVOLVING_ACCT").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_REVOLVING
    //===========================================================================================================================================
    val ICR_REVOLVING =
    """
      |    select
      |    PRH.PA01.PA01A.PA01AI01                 as report_id,
      |    cast(PCO.PC02.PC02G.PC02GS01 as bigint)                 as rev_org_num,
      |    cast(PCO.PC02.PC02G.PC02GS02 as bigint)                 as rev_acct_cnt,
      |    cast(PCO.PC02.PC02G.PC02GJ01 as decimal(18,2))                 as rev_credit_amt,
      |    cast(PCO.PC02.PC02G.PC02GJ02 as decimal(18,2))                 as rev_bal,
      |    cast(PCO.PC02.PC02G.PC02GJ03 as decimal(18,2))                 as rev_6mon_avg,
      |    '2020-09-27'                            as STATISTICS_DT
      |      from ods_table
      |""".stripMargin
    createView(tableEnv, ICR_REVOLVING, "ICR_REVOLVING")
    //tableEnv.sqlQuery("select * from ICR_REVOLVING").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_CREDITCARD
    //===========================================================================================================================================
    val ICR_CREDITCARD =
    """
      |    select
      |    PRH.PA01.PA01A.PA01AI01                 as report_id,
      |    cast(PCO.PC02.PC02H.PC02HS01 as bigint)                 as ccd_org_num,
      |    cast(PCO.PC02.PC02H.PC02HS02 as bigint)                 as ccd_acct_cnt,
      |    cast(PCO.PC02.PC02H.PC02HJ01 as decimal(18,2))                 as ccd_credit_amt,
      |    cast(PCO.PC02.PC02H.PC02HJ02 as decimal(18,2))                 as ccd_max_bal,
      |    cast(PCO.PC02.PC02H.PC02HJ03 as decimal(18,2))                 as ccd_min_bal,
      |    cast(PCO.PC02.PC02H.PC02HJ04 as decimal(18,2))                 as ccd_bal,
      |    cast(PCO.PC02.PC02H.PC02HJ05 as decimal(18,2))                 as ccd_6mon_avg,
      |    '2020-09-27'                            as STATISTICS_DT
      |      from ods_table
      |""".stripMargin
    createView(tableEnv, ICR_CREDITCARD, "ICR_CREDITCARD")
    //tableEnv.sqlQuery("select * from ICR_CREDITCARD").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_QUASI_CREDITCARD
    //===========================================================================================================================================
    val ICR_QUASI_CREDITCARD =
    """
      |    select
      |    PRH.PA01.PA01A.PA01AI01                 as report_id,
      |    cast(PCO.PC02.PC02I.PC02IS01 as bigint)                 as qua_org_num,
      |    cast(PCO.PC02.PC02I.PC02IS02 as bigint)                 as qua_acct_cnt,
      |    cast(PCO.PC02.PC02I.PC02IJ01 as decimal(18,2))                 as qua_credit_amtt,
      |    cast(PCO.PC02.PC02I.PC02IJ02 as decimal(18,2))                 as qua_max_bal,
      |    cast(PCO.PC02.PC02I.PC02IJ03 as decimal(18,2))                 as qua_min_bal,
      |    cast(PCO.PC02.PC02I.PC02IJ04 as decimal(18,2))                 as qua_bal,
      |    cast(PCO.PC02.PC02I.PC02IJ05 as decimal(18,2))                 as qua_6mon_avg,
      |    '2020-09-27'                            as STATISTICS_DT
      |      from ods_table
      |""".stripMargin
    createView(tableEnv, ICR_QUASI_CREDITCARD, "ICR_QUASI_CREDITCARD")
    //tableEnv.sqlQuery("select * from ICR_QUASI_CREDITCARD").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_REPAY_DUTY_NUM
    //===========================================================================================================================================
    val ICR_REPAY_DUTY_NUM =
    """
      |    select
      |    PRH.PA01.PA01A.PA01AI01                 as report_id,
      |    cast(PCO.PC02.PC02K.PC02KS01 as bigint)                 as rep_duty_num,
      |    '2020-09-27'                            as STATISTICS_DT
      |      from ods_table
      |""".stripMargin
    createView(tableEnv, ICR_REPAY_DUTY_NUM, "ICR_REPAY_DUTY_NUM")
    //tableEnv.sqlQuery("select * from ICR_REPAY_DUTY_NUM").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_POSTPAID_NUM
    //===========================================================================================================================================
    val ICR_POSTPAID_NUM =
    """
      |    select
      |    PRH.PA01.PA01A.PA01AI01                 as report_id,
      |    cast(PNO.PC03.PC030S01 as bigint)                 as pos_type_num,
      |    '2020-09-27'                            as STATISTICS_DT
      |      from ods_table
      |""".stripMargin
    createView(tableEnv, ICR_POSTPAID_NUM, "ICR_POSTPAID_NUM")
    //tableEnv.sqlQuery("select * from ICR_POSTPAID_NUM").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_PUBLIC_TYPE_NUM
    //===========================================================================================================================================
    val ICR_PUBLIC_TYPE_NUM =
    """
      |    select
      |    PRH.PA01.PA01A.PA01AI01                 as report_id,
      |    cast(PPO.PC04.PC040S01 as bigint)                 as pub_type_num,
      |    '2020-09-27'                            as STATISTICS_DT
      |      from ods_table
      |""".stripMargin
    createView(tableEnv, ICR_PUBLIC_TYPE_NUM, "ICR_PUBLIC_TYPE_NUM")
    //tableEnv.sqlQuery("select * from ICR_PUBLIC_TYPE_NUM").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_LAST_QUERY
    //===========================================================================================================================================
    val ICR_LAST_QUERY =
    """
      |    select
      |    PRH.PA01.PA01A.PA01AI01                 as report_id,
      |    PQO.PC05.PC05A.PC05AR01                 as lastque_dt,
      |    PQO.PC05.PC05A.PC05AD01                 as lastque_org_type,
      |    PQO.PC05.PC05A.PC05AI01                 as lastque_org_id,
      |    PQO.PC05.PC05A.PC05AQ01                 as lastque_reason,
      |    '2020-09-27'                            as STATISTICS_DT
      |      from ods_table
      |""".stripMargin
    createView(tableEnv, ICR_LAST_QUERY, "ICR_LAST_QUERY")
    //tableEnv.sqlQuery("select * from ICR_LAST_QUERY").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_QUERY
    //===========================================================================================================================================
    val ICR_QUERY =
    """
      |    select
      |    PRH.PA01.PA01A.PA01AI01                 as report_id,
      |    cast(PQO.PC05.PC05B.PC05BS01 as bigint)                 as que_1mon_org,
      |    cast(PQO.PC05.PC05B.PC05BS02 as bigint)                 as que_1mon_org_ccd,
      |    cast(PQO.PC05.PC05B.PC05BS03 as bigint)                 as que_1mon_cnt,
      |    cast(PQO.PC05.PC05B.PC05BS04 as bigint)                 as que_1mon_cnt_ccd,
      |    cast(PQO.PC05.PC05B.PC05BS05 as bigint)                 as que_1mon_cnt_self,
      |    cast(PQO.PC05.PC05B.PC05BS06 as bigint)                 as que_2year_cnt,
      |    cast(PQO.PC05.PC05B.PC05BS07 as bigint)                 as que_2year_cnt_guar,
      |    cast(PQO.PC05.PC05B.PC05BS08 as bigint)                 as que_2year_cnt_mer,
      |    '2020-09-27'                            as STATISTICS_DT
      |      from ods_table
      |""".stripMargin
    createView(tableEnv, ICR_QUERY, "ICR_QUERY")
    //tableEnv.sqlQuery("select * from ICR_QUERY").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_REPAYMENT_DUTY
    //===========================================================================================================================================
    val ICR_REPAYMENT_DUTY =
    """
      |select
      |t1.report_id                            as report_id,
      |info.PC02KD01                           as rep_iden_type,
      |info.PC02KD02                           as rep_duty_type_cd,
      |cast(info.PC02KS02 as bigint)           as rep_acct_cnt,
      |cast(info.PC02KJ01 as decimal(18,2))    as rep_amt,
      |cast(info.PC02KJ02 as decimal(18,2))    as rep_bal,
      |t1.SID                                  as SID,
      |t1.STATISTICS_DT                        as STATISTICS_DT
      |from(
      |select
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |PCO.PC02.PC02K.PC02KH         as data,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PC02KD01,PC02KD02,PC02KS02,PC02KJ01,PC02KJ02)
      |""".stripMargin
    createView(tableEnv, ICR_REPAYMENT_DUTY, "ICR_REPAYMENT_DUTY")
    //tableEnv.sqlQuery("select * from ICR_REPAYMENT_DUTY").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_POSTPAID
    //===========================================================================================================================================
    val ICR_POSTPAID =
    """
      |select
      |t1.report_id                            as report_id,
      |info.PC030D01                           as    pos_type_cd,
      |cast(info.PC030S02 as bigint)           as    pos_acct_cnt,
      |cast(info.PC030J01 as decimal(18,2))    as    pos_bal,
      |t1.SID                                  as SID,
      |t1.STATISTICS_DT                        as STATISTICS_DT
      |from(
      |select
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |PNO.PC030H         as data,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PC030D01,PC030S02,PC030J01)
      |""".stripMargin
    createView(tableEnv, ICR_POSTPAID, "ICR_POSTPAID")
    //tableEnv.sqlQuery("select * from ICR_POSTPAID").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_PUBLIC
    //===========================================================================================================================================
    val ICR_PUBLIC =
    """
      |select
      |t1.report_id                            as report_id,
      |info.PC040D01                           as pub_type_cd,
      |cast(info.PC040S02 as bigint)           as pub_cnt,
      |cast(info.PC040J01 as decimal(18,2))    as pub_amt,
      |t1.SID                                  as SID,
      |t1.STATISTICS_DT                        as STATISTICS_DT
      |from(
      |select
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |PPO.PC040H         as data,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PC040D01,PC040S02,PC040J01)
      |""".stripMargin
    createView(tableEnv, ICR_PUBLIC, "ICR_PUBLIC")
    //tableEnv.sqlQuery("select * from ICR_PUBLIC").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_QUERY_RECORD
    //===========================================================================================================================================
    val ICR_QUERY_RECORD =
    """
      |select
      |t1.report_id                            as report_id,
      |info.PH010R01                           as que_dt,
      |info.PH010D01                           as que_org_type,
      |info.PH010Q02                           as que_org_id,
      |info.PH010Q03                           as que_reason,
      |t1.SID                                  as SID,
      |t1.STATISTICS_DT                        as STATISTICS_DT
      |from (
      |select
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |POQ.PH01         as data,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PH010R01,PH010D01,PH010Q02,PH010Q03)
      |""".stripMargin
    createView(tableEnv, ICR_QUERY_RECORD, "ICR_QUERY_RECORD")
    //tableEnv.sqlQuery("select * from ICR_QUERY_RECORD").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_LOAN_INFO
    //===========================================================================================================================================
    val ICR_LOAN_INFO =
    """
      |select
      |t1.report_id                            as report_id,
      |info.PD01A.PD01AI01                     as acct_num,
      |info.PD01A.PD01AD01                     as acct_type_cd,
      |info.PD01A.PD01AD02                     as acct_org_type_cd,
      |info.PD01A.PD01AI02                     as acct_org_id,
      |info.PD01A.PD01AI03                     as acct_sign,
      |info.PD01A.PD01AI04                     as acct_agmt_num,
      |info.PD01A.PD01AD03                     as busi_spec_cd,
      |info.PD01A.PD01AR01                     as open_dt,
      |info.PD01A.PD01AD04                     as currency_cd,
      |cast(info.PD01A.PD01AJ01 as decimal(18,2)) as due_amt,
      |cast(info.PD01A.PD01AJ02 as decimal(18,2)) as credit_amt,
      |cast(info.PD01A.PD01AJ03 as decimal(18,2)) as credit_amt_share,
      |info.PD01A.PD01AR02                     as due_dt,
      |info.PD01A.PD01AD05                     as repay_mode_cd,
      |info.PD01A.PD01AD06                     as repay_frequency_cd,
      |cast(info.PD01A.PD01AS01 as bigint)     as repay_period,
      |info.PD01A.PD01AD07                     as grt_mode_cd,
      |info.PD01A.PD01AD08                     as lending_mode_cd,
      |info.PD01A.PD01AD09                     as share_sign_cd,
      |t1.SID                                  as SID,
      |t1.STATISTICS_DT                        as STATISTICS_DT
      |from(
      |select
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |PDA.PD01                                as data,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PD01A,PD01B,PD01C,PD01D,PD01E,PD01F,PD01G,PD01H,PD01Z)
      |""".stripMargin
    createView(tableEnv, ICR_LOAN_INFO, "ICR_LOAN_INFO")
    //tableEnv.sqlQuery("select * from ICR_LOAN_INFO").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_LOAN_LATEST
    //===========================================================================================================================================
    val ICR_LOAN_LATEST =
    """
      |select
      |t1.report_id                            as report_id,
      |info.PD01A.PD01AI01                     as acct_num,
      |info.PD01B.PD01BD01                     as acct_stat,
      |info.PD01B.PD01BR01                     as close_dt,
      |info.PD01B.PD01BR04                     as out_month,
      |cast(info.PD01B.PD01BJ01 as decimal(18,2))  as loan_bal,
      |info.PD01B.PD01BR02                     as latest_repay_dt,
      |cast(info.PD01B.PD01BJ02 as decimal(18,2)) as latest_repay_amt,
      |info.PD01B.PD01BD03                     as five_class_cd,
      |info.PD01B.PD01BD04                     as repay_stat,
      |info.PD01B.PD01BR03                     as report_dt,
      |t1.SID                                  as SID,
      |t1.STATISTICS_DT                        as STATISTICS_DT
      |from(
      |select
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |PDA.PD01                                as data,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PD01A,PD01B,PD01C,PD01D,PD01E,PD01F,PD01G,PD01H,PD01Z)
      |""".stripMargin
    createView(tableEnv, ICR_LOAN_LATEST, "ICR_LOAN_LATEST")
    //tableEnv.sqlQuery("select * from ICR_LOAN_LATEST").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_LOAN_1MONTH
    //===========================================================================================================================================
    val ICR_LOAN_1MONTH =
    """
      |select
      |t1.report_id                                as report_id,
      |info.PD01A.PD01AI01                         as acct_num,
      |info.PD01C.PD01CR01                         as mmonth,
      |info.PD01C.PD01CD01                         as acct_stat_1m,
      |cast(info.PD01C.PD01CJ01 as decimal(18,2))   as loan_bal_1m,
      |cast(info.PD01C.PD01CJ02 as decimal(18,2))   as credit_amt_used,
      |cast(info.PD01C.PD01CJ03 as decimal(18,2))   as credit_amt_big,
      |info.PD01C.PD01CD02                         as five_class_1m,
      |cast(info.PD01C.PD01CS01 as bigint)         as repay_period_left,
      |info.PD01C.PD01CR02                         as repay_date,
      |cast(info.PD01C.PD01CJ04 as bigint)         as repay_amt,
      |cast(info.PD01C.PD01CJ05 as bigint)         as repay_amt_act,
      |info.PD01C.PD01CR03                         as repay_date_latest,
      |cast(info.PD01C.PD01CS02 as bigint)         as overdue_period,
      |cast(info.PD01C.PD01CJ06 as decimal(18,2))  as overdue_amt,
      |cast(info.PD01C.PD01CJ07 as decimal(18,2))  as overdue_prin_31Amt,
      |cast(info.PD01C.PD01CJ08 as decimal(18,2))  as overdue_prin_61Amt,
      |cast(info.PD01C.PD01CJ09 as decimal(18,2))  as overdue_prin_91Amt,
      |cast(info.PD01C.PD01CJ10 as decimal(18,2))  as overdue_prin_180Amt,
      |cast(info.PD01C.PD01CJ11 as decimal(18,2))  as overdraft_prin_180Amt,
      |cast(info.PD01C.PD01CJ12 as decimal(18,2))  as credit_6mon_avg,
      |cast(info.PD01C.PD01CJ13 as decimal(18,2))  as overdraft_6mon_avg,
      |cast(info.PD01C.PD01CJ14 as decimal(18,2))  as max_credit_used,
      |cast(info.PD01C.PD01CJ15 as decimal(18,2))  as max_overdraft,
      |info.PD01C.PD01CR04                         as report_dt_1mon,
      |t1.SID                                  as SID,
      |t1.STATISTICS_DT                        as STATISTICS_DT
      |from(
      |select
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |PDA.PD01                                as data,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PD01A,PD01B,PD01C,PD01D,PD01E,PD01F,PD01G,PD01H,PD01Z)
      |""".stripMargin
    createView(tableEnv, ICR_LOAN_1MONTH, "ICR_LOAN_1MONTH")
    //tableEnv.sqlQuery("select * from ICR_LOAN_1MONTH").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_LOAN_24MONTH_DATE
    //===========================================================================================================================================
    val ICR_LOAN_24MONTH_DATE =
    """
      |select
      |t1.report_id                                as report_id,
      |info.PD01A.PD01AI01                         as acct_num,
      |info.PD01D.PD01DR01                         as start_dt,
      |info.PD01D.PD01DR02                         as end_dt,
      |t1.SID                                      as SID,
      |t1.STATISTICS_DT                            as STATISTICS_DT
      |from(
      |select
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |PDA.PD01                                as data,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PD01A,PD01B,PD01C,PD01D,PD01E,PD01F,PD01G,PD01H,PD01Z)
      |""".stripMargin
    createView(tableEnv, ICR_LOAN_24MONTH_DATE, "ICR_LOAN_24MONTH_DATE")
    //tableEnv.sqlQuery("select * from ICR_LOAN_24MONTH_DATE").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo 中间表 PDAPD01
    //===========================================================================================================================================
    val PDAPD01 =
    """
      |select
      |t1.report_id as report_id,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT,
      |info.PD01A  as PD01A,
      |info.PD01B  as PD01B,
      |info.PD01C  as PD01C,
      |info.PD01D  as PD01D,
      |info.PD01E  as PD01E,
      |info.PD01F  as PD01F,
      |info.PD01G  as PD01G,
      |info.PD01H  as PD01H,
      |info.PD01Z  as PD01Z
      |from(
      |select
      |PDA.PD01   as data,
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PD01A,PD01B,PD01C,PD01D,PD01E,PD01F,PD01G,PD01H,PD01Z)
      |""".stripMargin
    createView(tableEnv, PDAPD01, "PDAPD01")
    //tableEnv.sqlQuery("select * from PDAPD01").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo 中间表 PCAPD02
    //===========================================================================================================================================
    val PCAPD02 =
    """
      |select
      |t1.report_id as report_id,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT,
      |info.PD02A as PD02A,
      |info.PD02Z as PD02Z
      |from(
      |select
      |PCA.PD02   as data,
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PD02A,PD02Z)
      |""".stripMargin

    createView(tableEnv, PCAPD02, "PCAPD02")
    //tableEnv.sqlQuery("select * from PCAPD02").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo 中间表 PCRPD03
    //===========================================================================================================================================
    val PCRPD03 =
    """
      |select
      |t1.report_id as report_id,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT,
      |info.PD03A as PD03A,
      |info.PD03Z as PD03Z
      |from(
      |select
      |PCR.PD03   as data,
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PD03A,PD03Z)
      |""".stripMargin

    createView(tableEnv, PCRPD03, "PCRPD03")
    //===========================================================================================================================================
    //                                                          todo 中间表 PNDPE01
    //===========================================================================================================================================
    val PNDPE01 =
    """
      |select
      |t1.report_id as report_id,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT,
      |info.PE01A as PE01A,
      |info.PE01Z as PE01Z
      |from(
      |select
      |PND.PE01   as data,
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PE01A,PE01Z)
      |""".stripMargin
    createView(tableEnv, PNDPE01, "PNDPE01")

    //===========================================================================================================================================
    //                                                          todo 中间表 POTPF01
    //===========================================================================================================================================
    val POTPF01 =
    """
      |select
      |t1.report_id as report_id,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT,
      |info.PF01A as PF01A,
      |info.PF01Z as PF01Z
      |from(
      |select
      |POT.PF01   as data,
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PF01A,PF01Z)
      |""".stripMargin
    createView(tableEnv, POTPF01, "POTPF01")


    //===========================================================================================================================================
    //                                                          todo 中间表 PCJPF02
    //===========================================================================================================================================
    val PCJPF02 =
    """
      |select
      |t1.report_id as report_id,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT,
      |info.PF02A as PF02A,
      |info.PF02Z as PF02Z
      |from(
      |select
      |PCJ.PF02   as data,
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PF02A,PF02Z)
      |""".stripMargin
    createView(tableEnv, PCJPF02, "PCJPF02")
    //===========================================================================================================================================
    //                                                          todo 中间表 PCEPF03
    //===========================================================================================================================================
    val PCEPF03 =
    """
      |select
      |t1.report_id as report_id,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT,
      |info.PF03A as PF03A,
      |info.PF03Z as PF03Z
      |from(
      |select
      |PCE.PF03   as data,
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PF03A,PF03Z)
      |""".stripMargin
    createView(tableEnv, PCEPF03, "PCEPF03")
    //===========================================================================================================================================
    //                                                          todo 中间表 PAPPF04
    //===========================================================================================================================================
    val PAPPF04 =
    """
      |select
      |t1.report_id as report_id,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT,
      |info.PF04A as PF04A,
      |info.PF04Z as PF04Z
      |from(
      |select
      |PAP.PF04   as data,
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PF04A,PF04Z)
      |""".stripMargin
    createView(tableEnv, PAPPF04, "PAPPF04")
    //===========================================================================================================================================
    //                                                          todo 中间表 PHFPF05
    //===========================================================================================================================================
    val PHFPF05 =
    """
      |select
      |t1.report_id as report_id,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT,
      |info.PF05A as PF05A,
      |info.PF05Z as PF05Z
      |from(
      |select
      |PHF.PF05   as data,
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PF05A,PF05Z)
      |""".stripMargin
    createView(tableEnv, PHFPF05, "PHFPF05")

    //===========================================================================================================================================
    //                                                          todo 中间表 PBSPF06
    //===========================================================================================================================================

    val PBSPF06 =
      """
        |select
        |t1.report_id as report_id,
        |t1.SID as SID,
        |t1.STATISTICS_DT as STATISTICS_DT,
        |info.PF06A as PF06A,
        |info.PF06Z as PF06Z
        |from(
        |select
        |PBS.PF06   as data,
        |PRH.PA01.PA01A.PA01AI01  				as report_id,
        |PRH.PA01.PA01A.PA01AI01  				as SID,
        |'2020-09-27'                            as STATISTICS_DT
        |from ods_table
        |)t1,unnest(t1.data) as info(PF06A,PF06Z)
        |""".stripMargin
    createView(tableEnv, PBSPF06, "PBSPF06")
    //===========================================================================================================================================
    //                                                          todo 中间表 PPQPF07
    //===========================================================================================================================================

    val PPQPF07 =
      """
        |select
        |t1.report_id as report_id,
        |t1.SID as SID,
        |t1.STATISTICS_DT as STATISTICS_DT,
        |info.PF07A as PF07A,
        |info.PF07Z as PF07Z
        |from(
        |select
        |PPQ.PF07   as data,
        |PRH.PA01.PA01A.PA01AI01  				as report_id,
        |PRH.PA01.PA01A.PA01AI01  				as SID,
        |'2020-09-27'                            as STATISTICS_DT
        |from ods_table
        |)t1,unnest(t1.data) as info(PF07A,PF07Z)
        |""".stripMargin
    createView(tableEnv, PPQPF07, "PPQPF07")
    //===========================================================================================================================================
    //                                                          todo 中间表 PAHPF08
    //===========================================================================================================================================
    val PAHPF08 =
    """
      |select
      |t1.report_id as report_id,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT,
      |info.PF08A as PF08A,
      |info.PF08Z as PF08Z
      |from(
      |select
      |PAH.PF08   as data,
      |PRH.PA01.PA01A.PA01AI01  				as report_id,
      |PRH.PA01.PA01A.PA01AI01  				as SID,
      |'2020-09-27'                            as STATISTICS_DT
      |from ods_table
      |)t1,unnest(t1.data) as info(PF08A,PF08Z)
      |""".stripMargin
    createView(tableEnv, PAHPF08, "PAHPF08")

    //===========================================================================================================================================
    //                                                          todo ICR_LOAN_24MONTH
    //===========================================================================================================================================
    val ICR_LOAN_24MONTH =
    """
      |select
      |t1.report_id                                    as report_id,
      |t1.acct_num                                     as acct_num,
      |info.PD01DR03                                   as month_24m,
      |info.PD01DD01                                   as repay_stat_24m,
      |t1.SID  				                        as SID,
      |t1.STATISTICS_DT                                as STATISTICS_DT
      |from(
      |select
      |PD01A.PD01AI01 as acct_num,
      |report_id as report_id,
      |SID as SID,
      |STATISTICS_DT as  STATISTICS_DT,
      |PD01D.PD01DH as data
      |from PDAPD01
      |)t1,unnest(t1.data) as info(PD01DR03,PD01DD01)
      |""".stripMargin
    createView(tableEnv, ICR_LOAN_24MONTH, "ICR_LOAN_24MONTH")
    //    tableEnv.sqlQuery("select * from ICR_LOAN_24MONTH").toAppendStream[Row].print()


    //===========================================================================================================================================
    //                                                          todo ICR_LOAN_5YEARS_DATE
    //===========================================================================================================================================
    val ICR_LOAN_5YEARS_DATE =
    """
      |select
      |report_id as report_id,
      |PD01A.PD01AI01 as acct_num,
      |PD01E.PD01ER01 as start_dt_5y,
      |PD01E.PD01ER02 as end_dt_5y,
      |cast(PD01E.PD01ES01 as bigint) as mon_num,
      |SID as SID,
      |STATISTICS_DT as STATISTICS_DT
      |from PDAPD01
      |""".stripMargin
    createView(tableEnv, ICR_LOAN_5YEARS_DATE, "ICR_LOAN_5YEARS_DATE")
    //tableEnv.sqlQuery("select * from ICR_LOAN_5YEARS_DATE").toAppendStream[Row].print()


    //===========================================================================================================================================
    //                                                          todo ICR_LOAN_5YEARS
    //===========================================================================================================================================
    val ICR_LOAN_5YEARS =
    """
      |select
      |t1.report_id                                    as report_id,
      |t1.acct_num                                     as acct_num,
      |info.PD01ER03                                   as month_5y,
      |info.PD01ED01                                   as repay_stat_5y,
      |cast(info.PD01EJ01 as decimal(18,2))            as amt_5y,
      |t1.SID  				                        as SID,
      |t1.STATISTICS_DT                                as STATISTICS_DT
      |from(
      |select
      |PD01A.PD01AI01      as acct_num,
      |report_id 			as report_id,
      |SID 				as SID,
      |STATISTICS_DT 		as STATISTICS_DT,
      |PD01E.PD01EH        as data
      |from PDAPD01
      |)t1,unnest(t1.data) as info(PD01ER03,PD01ED01,PD01EJ01)
      |""".stripMargin
    createView(tableEnv, ICR_LOAN_5YEARS, "ICR_LOAN_5YEARS")
    //tableEnv.sqlQuery("select * from ICR_LOAN_5YEARS").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_SPECIAL_TXN_NUM
    //===========================================================================================================================================
    val ICR_SPECIAL_TXN_NUM =
    """
      |select
      |report_id as report_id,
      |PD01A.PD01AI01      as acct_num,
      |cast(PD01F.PD01FS01 as bigint) as spe_txn_num,
      |SID as SID,
      |STATISTICS_DT as STATISTICS_DT
      |from PDAPD01
      |""".stripMargin
    createView(tableEnv, ICR_SPECIAL_TXN_NUM, "ICR_SPECIAL_TXN_NUM")
    //tableEnv.sqlQuery("select * from ICR_SPECIAL_TXN_NUM").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_SPECIAL_TXN
    //===========================================================================================================================================
    val ICR_SPECIAL_TXN =
    """
      |select
      |t1.report_id as report_id,
      |t1.acct_num as acct_num,
      |info.PD01FD01 as spe_txn_type_cd,
      |info.PD01FR01 as spe_txn_dt,
      |cast(info.PD01FS02 as bigint) as spe_end_dt_change,
      |cast(info.PD01FJ01 as decimal(18,2)) as spe_amt,
      |info.PD01FQ01 as spe_record,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT
      |from(
      |select
      |PD01A.PD01AI01      as acct_num,
      |report_id 			as report_id,
      |SID 				as SID,
      |STATISTICS_DT 		as STATISTICS_DT,
      |PD01F.PD01FH     as data
      |from PDAPD01
      |)t1,unnest(t1.data) as info(PD01FD01,PD01FR01,PD01FS02,PD01FJ01,PD01FQ01)
      |""".stripMargin
    createView(tableEnv, ICR_SPECIAL_TXN, "ICR_SPECIAL_TXN")
    //tableEnv.sqlQuery("select * from ICR_SPECIAL_TXN").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_SPECIAL_EVENT_NUM
    //===========================================================================================================================================
    val ICR_SPECIAL_EVENT_NUM =
    """
      |select
      |report_id as report_id,
      |PD01A.PD01AI01      as acct_num,
      |cast(PD01G.PD01GS01 as bigint) as spe_event_num,
      |SID as SID,
      |STATISTICS_DT as STATISTICS_DT
      |from PDAPD01
      |""".stripMargin
    createView(tableEnv, ICR_SPECIAL_EVENT_NUM, "ICR_SPECIAL_EVENT_NUM")
    //tableEnv.sqlQuery("select * from ICR_SPECIAL_EVENT_NUM").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_SPECIAL_EVENT
    //===========================================================================================================================================
    val ICR_SPECIAL_EVENT =
    """
      |select
      |t1.report_id as report_id,
      |t1.acct_num as acct_num,
      |info.PD01GR01 as spe_event_dt,
      |info.PD01GD01 as spe_event_type,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT
      |from(
      |select
      |report_id 			as report_id,
      |PD01A.PD01AI01      as acct_num,
      |SID 				as SID,
      |STATISTICS_DT 		as STATISTICS_DT,
      |PD01G.PD01GH     as data
      |from PDAPD01
      |)t1,unnest(t1.data) as info(PD01GR01,PD01GD01)
      |""".stripMargin
    createView(tableEnv, ICR_SPECIAL_EVENT, "ICR_SPECIAL_EVENT")
    //tableEnv.sqlQuery("select * from ICR_SPECIAL_EVENT").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_LARGE_INSTALMENTS_NUM
    //===========================================================================================================================================
    val ICR_LARGE_INSTALMENTS_NUM =
    """
      |select
      |report_id as report_id,
      |PD01A.PD01AI01      as acct_num,
      |cast(PD01H.PD01HS01 as bigint) as large_cnt,
      |SID as SID,
      |STATISTICS_DT as STATISTICS_DT
      |from PDAPD01
      |""".stripMargin
    createView(tableEnv, ICR_LARGE_INSTALMENTS_NUM, "ICR_LARGE_INSTALMENTS_NUM")
    //tableEnv.sqlQuery("select * from ICR_LARGE_INSTALMENTS_NUM").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_LARGE_INSTALMENTS
    //===========================================================================================================================================
    val ICR_LARGE_INSTALMENTS =
    """
      |select
      |t1.report_id as report_id,
      |t1.acct_num as acct_num,
      |cast(info.PD01HJ01 as decimal(18,2)) as large_amt,
      |info.PD01HR01 as large_eff_dt,
      |info.PD01HR02 as large_end_dt,
      |cast(info.PD01HJ02 as decimal(18,2)) as large_amt_used,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT
      |from(
      |select
      |report_id 			as report_id,
      |PD01A.PD01AI01      as acct_num,
      |SID 				as SID,
      |STATISTICS_DT 		as STATISTICS_DT,
      |PD01H.PD01HH     as data
      |from PDAPD01
      |)t1,unnest(t1.data) as info(PD01HJ01,PD01HR01,PD01HR02,PD01HJ02)
      |""".stripMargin
    createView(tableEnv, ICR_LARGE_INSTALMENTS, "ICR_LARGE_INSTALMENTS")
    //tableEnv.sqlQuery("select * from ICR_LARGE_INSTALMENTS").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_LOAN_DECLARE_NUM
    //===========================================================================================================================================
    val ICR_LOAN_DECLARE_NUM =
    """
      |select
      |report_id as report_id,
      |PD01A.PD01AI01      as acct_num,
      |cast(PD01Z.PD01ZS01 as bigint) as declare_num,
      |SID as SID,
      |STATISTICS_DT as STATISTICS_DT
      |from PDAPD01
      |""".stripMargin
    createView(tableEnv, ICR_LOAN_DECLARE_NUM, "ICR_LOAN_DECLARE_NUM")
    //    tableEnv.sqlQuery("select * from ICR_LOAN_DECLARE_NUM").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_LOAN_DECLARE
    //===========================================================================================================================================
    val ICR_LOAN_DECLARE =
    """
      |select
      |t1.report_id as report_id,
      |t1.acct_num as acct_num,
      |info.PD01ZD01 as declare_type_cd,
      |info.PD01ZQ01 as declare_content,
      |info.PD01ZR01 as declare_dt,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT
      |from(
      |select
      |report_id 			as report_id,
      |PD01A.PD01AI01      as acct_num,
      |SID 				as SID,
      |STATISTICS_DT 		as STATISTICS_DT,
      |PD01Z.PD01ZH     as data
      |from PDAPD01
      |)t1,unnest(t1.data) as info(PD01ZD01,PD01ZQ01,PD01ZR01)
      |""".stripMargin
    createView(tableEnv, ICR_LOAN_DECLARE, "ICR_LOAN_DECLARE")
    //tableEnv.sqlQuery("select * from ICR_LOAN_DECLARE").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_CREDIT_AGREEMENT
    //===========================================================================================================================================
    val ICR_CREDIT_AGREEMENT =
    """
      |select
      |report_id as report_id,
      |PD02A.PD02AI01 as agmt_num,
      |PD02A.PD02AD01  as agmt_org_type,
      |PD02A.PD02AI02  as agmt_org_id,
      |PD02A.PD02AI03  as agmt_sign,
      |PD02A.PD02AD02  as agmt_use_cd,
      |cast(PD02A.PD02AJ01 as decimal(18,2))  as agmt_amt,
      |PD02A.PD02AD03  as agmt_currency_cd,
      |PD02A.PD02AR01  as agmt_eff_dt,
      |PD02A.PD02AR02  as agmt_due_dt,
      |PD02A.PD02AD04  as agmt_stat_cd,
      |cast(PD02A.PD02AJ04 as decimal(18,2))  as agmt_amt_used,
      |PD02A.PD02AI04  as agmt_limit_id,
      |cast(PD02A.PD02AJ03 as decimal(18,2))  as agmt_limit,
      |SID as SID,
      |STATISTICS_DT as STATISTICS_DT
      |from PCAPD02
      |""".stripMargin
    createView(tableEnv, ICR_CREDIT_AGREEMENT, "ICR_CREDIT_AGREEMENT")
    //    tableEnv.sqlQuery("select * from ICR_CREDIT_AGREEMENT").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_AGREEMENT_DECLARE_NUM
    //===========================================================================================================================================
    val ICR_AGREEMENT_DECLARE_NUM =
    """
      |select
      |report_id as report_id,
      |PD02A.PD02AI01 as agmt_num,
      |cast(PD02Z.PD02ZS01 as bigint) as agmt_dec_num,
      |SID as SID,
      |STATISTICS_DT as STATISTICS_DT
      |from PCAPD02
      |""".stripMargin
    createView(tableEnv, ICR_AGREEMENT_DECLARE_NUM, "ICR_AGREEMENT_DECLARE_NUM")
    //tableEnv.sqlQuery("select * from ICR_AGREEMENT_DECLARE_NUM").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_AGREEMENT_DECLARE
    //===========================================================================================================================================
    val ICR_AGREEMENT_DECLARE =
    """
      |select
      |t1.report_id as report_id,
      |t1.agmt_num as agmt_num,
      |info.PD02ZD01 as agmt_decl_type,
      |info.PD02ZQ01 as agmt_decl_cont,
      |info.PD02ZR01 as agmt_decl_dt,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT
      |from(
      |select
      |report_id 			as report_id,
      |PD02A.PD02AI01      as agmt_num,
      |SID 				as SID,
      |STATISTICS_DT 		as STATISTICS_DT,
      |PD02Z.PD02ZH     as data
      |from PCAPD02
      |)t1,unnest(t1.data) as info(PD02ZD01,PD02ZQ01,PD02ZR01)
      |""".stripMargin
    createView(tableEnv, ICR_AGREEMENT_DECLARE, "ICR_AGREEMENT_DECLARE")
    //    tableEnv.sqlQuery("select * from ICR_AGREEMENT_DECLARE").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_POSTPAID_INFO
    //===========================================================================================================================================
    val ICR_POSTPAID_INFO =
    """
      |select
      |report_id as report_id,
      |PE01A.PE01AD01 as postpaid_acct_type_cd,
      |PE01A.PE01AQ01 as postpaid_org_name,
      |PE01A.PE01AD02 as postpaid_busi_type_cd,
      |PE01A.PE01AR01 as postpaid_open_dt,
      |PE01A.PE01AD03 as postpaid_stat_cd,
      |cast(PE01A.PE01AJ01 as decimal(18,2)) as postpaid_owe_amt,
      |PE01A.PE01AR02 as postpaid_acct_mon,
      |PE01A.PE01AQ02 as postpaid_24mon,
      |STATISTICS_DT as STATISTICS_DT,
      |SID as SID
      |from PNDPE01
      |""".stripMargin
    createView(tableEnv, ICR_POSTPAID_INFO, "ICR_POSTPAID_INFO")
    //tableEnv.sqlQuery("select * from ICR_POSTPAID_INFO").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_POSTPAID_DECLARE_NUM
    //===========================================================================================================================================
    val ICR_POSTPAID_DECLARE_NUM =
    """
      |select
      |report_id as report_id,
      |cast(PE01Z.PE01ZS01 as bigint) as postpaid_decl_num,
      |STATISTICS_DT as STATISTICS_DT
      |from PNDPE01
      |""".stripMargin
    createView(tableEnv, ICR_POSTPAID_DECLARE_NUM, "ICR_POSTPAID_DECLARE_NUM")
    //tableEnv.sqlQuery("select * from ICR_POSTPAID_DECLARE_NUM").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_TAX_INFO
    //===========================================================================================================================================
    val ICR_TAX_INFO =
    """
      |select
      |report_id as report_id,
      |PF01A.PF01AQ01 as tax_org_id,
      |cast(PF01A.PF01AJ01 as decimal(18,2)) as tax_owe_amt,
      |PF01A.PF01AR01 as tax_owe_dt,
      |STATISTICS_DT as STATISTICS_DT
      |from POTPF01
      |""".stripMargin
    createView(tableEnv, ICR_TAX_INFO, "ICR_TAX_INFO")
    //tableEnv.sqlQuery("select * from ICR_TAX_INFO").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_TAX_DECLARE_NUM
    //===========================================================================================================================================
    val ICR_TAX_DECLARE_NUM =
    """
      |select
      |report_id as report_id,
      |cast(PF01Z.PF01ZS01 as bigint) as tax_owe_amt,
      |STATISTICS_DT as STATISTICS_DT
      |from POTPF01
      |""".stripMargin
    createView(tableEnv, ICR_TAX_DECLARE_NUM, "ICR_TAX_DECLARE_NUM")
    //    tableEnv.sqlQuery("select * from ICR_TAX_DECLARE_NUM").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_CIVIL_JUDGEMENT
    //===========================================================================================================================================
    val ICR_CIVIL_JUDGEMENT =
    """
      |select
      |report_id as report_id,
      |PF02A.PF02AQ01 as civ_court_id,
      |PF02A.PF02AQ02 as civ_reason,
      |PF02A.PF02AR01 as civ_reg_dt,
      |PF02A.PF02AD01 as civ_closed_cd,
      |PF02A.PF02AQ03 as civ_result,
      |PF02A.PF02AR02 as civ_eff_dt,
      |PF02A.PF02AQ04 as civ_target,
      |cast(PF02A.PF02AJ01 as decimal(18,2)) as civ_amt,
      |STATISTICS_DT as STATISTICS_DT,
      |SID as SID
      |from PCJPF02
      |""".stripMargin
    createView(tableEnv, ICR_CIVIL_JUDGEMENT, "ICR_CIVIL_JUDGEMENT")
    //    tableEnv.sqlQuery("select * from ICR_CIVIL_JUDGEMENT_DECLARE").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_CIV_JUDGE_DECLARE_NUM
    //===========================================================================================================================================
    val ICR_CIV_JUDGE_DECLARE_NUM =
    """
      |select
      |report_id as report_id,
      |cast(PF02Z.PF02ZS01 as bigint) as civ_decl_num,
      |STATISTICS_DT as STATISTICS_DT
      |from PCJPF02
      |""".stripMargin
    createView(tableEnv, ICR_CIV_JUDGE_DECLARE_NUM, "ICR_CIV_JUDGE_DECLARE_NUM")
    //    tableEnv.sqlQuery("select * from ICR_CIV_JUDGE_DECLARE_NUM").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_ENFORCEMENT
    //===========================================================================================================================================
    val ICR_ENFORCEMENT =
    """
      |select
      |report_id as report_id,
      |PF03A.PF03AQ01 as   enf_court_id,
      |PF03A.PF03AQ02 as   enf_reason,
      |PF03A.PF03AR01 as   enf_reg_dt,
      |PF03A.PF03AD01 as   enf_closed_cd,
      |PF03A.PF03AQ03 as   enf_stat,
      |PF03A.PF03AR02 as   enf_closed_dt,
      |PF03A.PF03AQ04 as   enf_apply_target,
      |cast(PF03A.PF03AJ01 as decimal(18,2)) as   enf_apply_amt,
      |PF03A.PF03AQ05 as   enf_execute_target,
      |cast(PF03A.PF03AJ02 as decimal(18,2)) as   enf_execute_amt,
      |STATISTICS_DT as STATISTICS_DT,
      |SID as SID
      |from PCEPF03
      |""".stripMargin
    createView(tableEnv, ICR_ENFORCEMENT, "ICR_ENFORCEMENT")
    // tableEnv.sqlQuery("select * from ICR_ENFORCEMENT").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_ENFORCEMENT_DECLARE_NUM
    //===========================================================================================================================================
    val ICR_ENFORCEMENT_DECLARE_NUM =
    """
      |select
      |report_id as report_id,
      |cast(PF03Z.PF03ZS01 as decimal(18,2)) as enf_decl_num,
      |STATISTICS_DT as STATISTICS_DT
      |from PCEPF03
      |""".stripMargin
    createView(tableEnv, ICR_ENFORCEMENT_DECLARE_NUM, "ICR_ENFORCEMENT_DECLARE_NUM")
    //    tableEnv.sqlQuery("select * from ICR_ENFORCEMENT_DECLARE_NUM").toAppendStream[Row].print()

    //===========================================================================================================================================
    //                                                          todo ICR_ADMINPUNISHMENT
    //===========================================================================================================================================
    val ICR_ADMINPUNISHMENT =
    """
      |select
      |report_id as report_id,
      |PF04A.PF04AQ01 as punish_org_id,
      |PF04A.PF04AQ02 as punish_cont,
      |cast(PF04A.PF04AJ01 as decimal(18,2)) as punish_amt,
      |PF04A.PF04AR01 as punish_eff_dt,
      |PF04A.PF04AR02 as punish_end_dt,
      |PF04A.PF04AQ03 as punish_result,
      |STATISTICS_DT as STATISTICS_DT,
      |SID as SID
      |from PAPPF04
      |""".stripMargin
    createView(tableEnv, ICR_ADMINPUNISHMENT, "ICR_ADMINPUNISHMENT")
    //   tableEnv.sqlQuery("select * from ICR_ADMINPUNISHMENT").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_PUNISHMENT_DECLARE_NUM
    //===========================================================================================================================================
    val ICR_PUNISHMENT_DECLARE_NUM =
    """
      |select
      |report_id as report_id,
      |cast(PF04Z.PF04ZS01 as bigint) as punish_decl_num,
      |STATISTICS_DT as STATISTICS_DT
      |from PAPPF04
      |""".stripMargin
    createView(tableEnv, ICR_PUNISHMENT_DECLARE_NUM, "ICR_PUNISHMENT_DECLARE_NUM")
    //    tableEnv.sqlQuery("select * from ICR_PUNISHMENT_DECLARE_NUM").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_HOUSINGFUND
    //===========================================================================================================================================
    val ICR_HOUSINGFUND =
    """
      |select
      |report_id as report_id,
      |PF05A.PF05AQ01 as housing_area,
      |PF05A.PF05AR01 as housing_dt,
      |PF05A.PF05AD01 as housing_stat_cd,
      |PF05A.PF05AR02 as housing_start_dt,
      |PF05A.PF05AR03 as housing_last_dt,
      |cast(PF05A.PF05AQ02 as bigint) as housing_prop,
      |cast(PF05A.PF05AQ03 as bigint) as housing_prop_indiv,
      |cast(PF05A.PF05AJ01 as bigint) as housing_amt,
      |PF05A.PF05AQ04 as housing_unit,
      |PF05A.PF05AR04 as housing_update_dt,
      |STATISTICS_DT as STATISTICS_DT,
      |SID as SID
      |from PHFPF05
      |""".stripMargin
    createView(tableEnv, ICR_HOUSINGFUND, "ICR_HOUSINGFUND")
    //    tableEnv.sqlQuery("select * from ICR_HOUSINGFUND").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_HOUSINGFUND_DECLARE_NUM
    //===========================================================================================================================================
    val ICR_HOUSINGFUND_DECLARE_NUM =
    """
      |select
      |report_id as report_id,
      |cast(PF05Z.PF05ZS01 as bigint) as housing_decl_num,
      |STATISTICS_DT as STATISTICS_DT
      |from PHFPF05
      |""".stripMargin
    createView(tableEnv, ICR_HOUSINGFUND_DECLARE_NUM, "ICR_HOUSINGFUND_DECLARE_NUM")
    //    tableEnv.sqlQuery("select * from ICR_HOUSINGFUND_DECLARE_NUM").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_ALLOWANCE
    //===========================================================================================================================================
    val ICR_ALLOWANCE =
    """
      |select
      |report_id as report_id,
      |PF06A.PF06AD01 as   allowance_type_cd,
      |PF06A.PF06AQ01 as   allowance_area,
      |PF06A.PF06AQ02 as   allowance_unit,
      |cast(PF06A.PF06AQ03 as bigint) as   allowance_income,
      |PF06A.PF06AR01 as   allowance_sup_dt,
      |PF06A.PF06AR02 as   allowance_app_dt,
      |PF06A.PF06AR03 as   allowance_update_dt,
      |STATISTICS_DT as STATISTICS_DT,
      |SID as SID
      |from PBSPF06
      |""".stripMargin
    createView(tableEnv, ICR_ALLOWANCE, "ICR_ALLOWANCE")
    //    tableEnv.sqlQuery("select * from ICR_ALLOWANCE").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_ALLOWANCE_DECLARE_NUM
    //===========================================================================================================================================
    val ICR_ALLOWANCE_DECLARE_NUM =
    """
      |select
      |report_id as report_id,
      |cast(PF06Z.PF06ZS01 as bigint) as allowance_decl_num,
      |STATISTICS_DT as STATISTICS_DT
      |from PBSPF06
      |""".stripMargin
    createView(tableEnv, ICR_ALLOWANCE_DECLARE_NUM, "ICR_ALLOWANCE_DECLARE_NUM")
    //    tableEnv.sqlQuery("select * from ICR_ALLOWANCE_DECLARE_NUM").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_QUALIFICATION
    //===========================================================================================================================================
    val ICR_QUALIFICATION =
    """
      |select
      |report_id      as report_id,
      |PF07A.PF07AQ01 as   qual_name,
      |PF07A.PF07AQ02 as   qual_org,
      |PF07A.PF07AD01 as   qual_level_cd,
      |PF07A.PF07AD02 as   qual_area,
      |PF07A.PF07AR01 as   qual_get_dt,
      |PF07A.PF07AR02 as   qual_due_dt,
      |PF07A.PF07AR03 as   qual_revoke_dt,
      |STATISTICS_DT as STATISTICS_DT,
      |SID as SID
      |from PPQPF07
      |""".stripMargin
    createView(tableEnv, ICR_QUALIFICATION, "ICR_QUALIFICATION")
    //    tableEnv.sqlQuery("select * from ICR_QUALIFICATION").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_QUALFICATION_DECL_NUM
    //===========================================================================================================================================
    val ICR_QUALFICATION_DECL_NUM =
    """
      |select
      |report_id as report_id,
      |cast(PF07Z.PF07ZS01 as bigint) as allowance_decl_num,
      |STATISTICS_DT as STATISTICS_DT
      |from PPQPF07
      |""".stripMargin
    createView(tableEnv, ICR_QUALFICATION_DECL_NUM, "ICR_QUALFICATION_DECL_NUM")
    //    tableEnv.sqlQuery("select * from ICR_QUALFICATION_DECL_NUM").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_REWARD
    //===========================================================================================================================================
    val ICR_REWARD =
    """
      |select
      |report_id      as   report_id,
      |PF08A.PF08AQ01 as   rew_org,
      |PF08A.PF08AQ02 as   rew_cont,
      |PF08A.PF08AR01 as   rew_eff_dt,
      |PF08A.PF08AR02 as   rew_end_dt,
      |STATISTICS_DT as STATISTICS_DT,
      |SID as SID
      |from PAHPF08
      |""".stripMargin
    createView(tableEnv, ICR_REWARD, "ICR_REWARD")
    //    tableEnv.sqlQuery("select * from ICR_REWARD").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_REWARD_DECLARE_NUM
    //===========================================================================================================================================
    val ICR_REWARD_DECLARE_NUM =
    """
      |select
      |report_id as report_id,
      |cast(PF08Z.PF08ZS01 as bigint) as allowance_decl_num,
      |STATISTICS_DT as STATISTICS_DT
      |from PAHPF08
      |""".stripMargin
    createView(tableEnv, ICR_REWARD_DECLARE_NUM, "ICR_REWARD_DECLARE_NUM")
    //    tableEnv.sqlQuery("select * from ICR_REWARD_DECLARE_NUM").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_POSTPAID_DECLARE
    //===========================================================================================================================================
    val ICR_POSTPAID_DECLARE =
    """
      |select
      |t1.report_id  as  report_id,
      |info.PE01ZD01 as  postpaid_decl_type,
      |info.PE01ZQ01 as  postpaid_decl_cont,
      |info.PE01ZR01 as  postpaid_decl_dt,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT
      |from(
      |select
      |report_id 			as report_id,
      |SID 				as SID,
      |STATISTICS_DT 		as STATISTICS_DT,
      |PE01Z.PE01ZH     as data
      |from PNDPE01
      |)t1,unnest(t1.data) as info(PE01ZD01,PE01ZQ01,PE01ZR01)
      |""".stripMargin
    createView(tableEnv, ICR_POSTPAID_DECLARE, "ICR_POSTPAID_DECLARE")
    //    tableEnv.sqlQuery("select * from ICR_POSTPAID_DECLARE").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_TAX_DECLARE
    //===========================================================================================================================================
    val ICR_TAX_DECLARE =
    """
      |
      |select
      |t1.report_id  as  report_id,
      |info.PF01ZD01 as tax_decl_type,
      |info.PF01ZQ01 as tax_decl_cont,
      |info.PF01ZR01 as tax_decl_dt,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT
      |from(
      |select
      |report_id 			as report_id,
      |SID 				as SID,
      |STATISTICS_DT 		as STATISTICS_DT,
      |PF01Z.PF01ZH     as data
      |from POTPF01
      |)t1,unnest(t1.data) as info(PF01ZD01,PF01ZQ01,PF01ZR01)
      |""".stripMargin
    createView(tableEnv, ICR_TAX_DECLARE, "ICR_TAX_DECLARE")
    //    tableEnv.sqlQuery("select * from ICR_TAX_DECLARE").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_CIVIL_JUDGEMENT_DECLARE
    //===========================================================================================================================================
    val ICR_CIVIL_JUDGEMENT_DECLARE =
    """
      |select
      |t1.report_id  as  report_id,
      |info.PF02ZD01 as civ_decl_type,
      |info.PF02ZQ01 as civ_decl_cont,
      |info.PF02ZR01 as civ_decl_dt,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT
      |from(
      |select
      |report_id 			as report_id,
      |SID 				as SID,
      |STATISTICS_DT 		as STATISTICS_DT,
      |PF02Z.PF02ZH     as data
      |from PCJPF02
      |)t1,unnest(t1.data) as info(PF02ZD01,PF02ZQ01,PF02ZR01)
      |""".stripMargin
    createView(tableEnv, ICR_CIVIL_JUDGEMENT_DECLARE, "ICR_CIVIL_JUDGEMENT_DECLARE")
    //    tableEnv.sqlQuery("select * from ICR_CIVIL_JUDGEMENT_DECLARE").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_ENFORCEMENT_DECLARE
    //===========================================================================================================================================
    val ICR_ENFORCEMENT_DECLARE =
    """
      |select
      |t1.report_id  as  report_id,
      |info.PF03ZD01 as enf_decl_type,
      |info.PF03ZQ01 as enf_decl_cont,
      |info.PF03ZR01 as enf_decl_dt,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT
      |from(
      |select
      |report_id 			as report_id,
      |SID 				as SID,
      |STATISTICS_DT 		as STATISTICS_DT,
      |PF03Z.PF03ZH        as data
      |from PCEPF03
      |)t1,unnest(t1.data) as info(PF03ZD01,PF03ZQ01,PF03ZR01)
      |""".stripMargin
    createView(tableEnv, ICR_ENFORCEMENT_DECLARE, "ICR_ENFORCEMENT_DECLARE")
    //    tableEnv.sqlQuery("select * from ICR_ENFORCEMENT_DECLARE").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_ADMINPUNISHMENT_DECLARE
    //===========================================================================================================================================
    val ICR_ADMINPUNISHMENT_DECLARE =
    """
      |select
      |t1.report_id  as  report_id,
      |info.PF04ZD01 as punish_decl_type,
      |info.PF04ZQ01 as punish_decl_cont,
      |info.PF04ZR01 as punish_decl_dt,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT
      |from(
      |select
      |report_id 			as report_id,
      |SID 				as SID,
      |STATISTICS_DT 		as STATISTICS_DT,
      |PF04Z.PF04ZH     as data
      |from PAPPF04
      |)t1,unnest(t1.data) as info(PF04ZD01,PF04ZQ01,PF04ZR01)
      |""".stripMargin
    createView(tableEnv, ICR_ADMINPUNISHMENT_DECLARE, "ICR_ADMINPUNISHMENT_DECLARE")
    //    tableEnv.sqlQuery("select * from ICR_ADMINPUNISHMENT_DECLARE").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_HOUSINGFUND_DECLARE
    //===========================================================================================================================================
    val ICR_HOUSINGFUND_DECLARE =
    """
      |select
      |t1.report_id  as  report_id,
      |info.PF05ZD01 as housing_decl_type,
      |info.PF05ZQ01 as housing_decl_cont,
      |info.PF05ZR01 as housing_decl_dt,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT
      |from(
      |select
      |report_id 			as report_id,
      |SID 				as SID,
      |STATISTICS_DT 		as STATISTICS_DT,
      |PF05Z.PF05ZH     as data
      |from PHFPF05
      |)t1,unnest(t1.data) as info(PF05ZD01,PF05ZQ01,PF05ZR01)
      |""".stripMargin
    createView(tableEnv, ICR_HOUSINGFUND_DECLARE, "ICR_HOUSINGFUND_DECLARE")
    //    tableEnv.sqlQuery("select * from ICR_HOUSINGFUND_DECLARE").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_ALLOWANCE_DECLARE
    //===========================================================================================================================================
    val ICR_ALLOWANCE_DECLARE =
    """
      |select
      |t1.report_id  as  report_id,
      |info.PF06ZD01 as allowance_decl_type,
      |info.PF06ZQ01 as allowance_decl_cont,
      |info.PF06ZR01 as allowance_decl_dt,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT
      |from(
      |select
      |report_id 			as report_id,
      |SID 				as SID,
      |STATISTICS_DT 		as STATISTICS_DT,
      |PF06Z.PF06ZH     as data
      |from PBSPF06
      |)t1,unnest(t1.data) as info(PF06ZD01,PF06ZQ01,PF06ZR01)
      |""".stripMargin
    createView(tableEnv, ICR_ALLOWANCE_DECLARE, "ICR_ALLOWANCE_DECLARE")
    //    tableEnv.sqlQuery("select * from ICR_ALLOWANCE_DECLARE").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_QUALIFICATION_DECLARE
    //===========================================================================================================================================
    val ICR_QUALIFICATION_DECLARE =
    """
      |select
      |t1.report_id  as  report_id,
      |info.PF07ZD01 as qual_decl_type,
      |info.PF07ZQ01 as qual_decl_cont,
      |info.PF07ZR01 as qual_decl_dt,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT
      |from(
      |select
      |report_id 			as report_id,
      |SID 				as SID,
      |STATISTICS_DT 		as STATISTICS_DT,
      |PF07Z.PF07ZH     as data
      |from PPQPF07
      |)t1,unnest(t1.data) as info(PF07ZD01,PF07ZQ01,PF07ZR01)
      |""".stripMargin
    createView(tableEnv, ICR_QUALIFICATION_DECLARE, "ICR_QUALIFICATION_DECLARE")
    //    tableEnv.sqlQuery("select * from ICR_QUALIFICATION_DECLARE").toAppendStream[Row].print()
    //===========================================================================================================================================
    //                                                          todo ICR_REWARD_DECLARE
    //===========================================================================================================================================
    val ICR_REWARD_DECLARE =
    """
      |select
      |t1.report_id  as  report_id,
      |info.PF08ZD01 as rew_decl_type,
      |info.PF08ZQ01 as rew_decl_cont,
      |info.PF08ZR01 as rew_decl_dt,
      |t1.SID as SID,
      |t1.STATISTICS_DT as STATISTICS_DT
      |from(
      |select
      |report_id 			as report_id,
      |SID 				as SID,
      |STATISTICS_DT 		as STATISTICS_DT,
      |PF08Z.PF08ZH     as data
      |from PAHPF08
      |)t1,unnest(t1.data) as info(PF08ZD01,PF08ZQ01,PF08ZR01)
      |""".stripMargin
    createView(tableEnv, ICR_REWARD_DECLARE, "ICR_REWARD_DECLARE")


    val sink_table3 =
      """
        |CREATE TABLE sink_table3 (
        |    a1 string,
        |    a2 string,
        |    a3 string,
        |    a4 string,
        |    a5 string,
        |    a6 string
        |    )
        |    WITH (
        |      'connector' = 'print'
        |      )
        |""".stripMargin
    tableEnv.executeSql(sink_table3)
    tableEnv.executeSql("insert into sink_table3 select * from ICR_REWARD_DECLARE")
    //tableEnv.sqlUpdate("insert into sink_table3 select * from ICR_REWARD_DECLARE")
    //
    //    val sink_table =
    //      """
    //        |CREATE TABLE source_table_2 (
    //        |    a1 string,
    //        |    a2 string,
    //        |    a3 string,
    //        |    a4 string,
    //        |    a5 string,
    //        |    a6 string
    //        |    )
    //        |    WITH (
    //        |      'connector' = 'kafka',
    //        |      'topic' = 'qinghuatest001',
    //        |      'properties.group.id'='dev_flink',
    //        |      'properties.zookeeper.connect'='10.1.30.6:2181',
    //        |      'properties.bootstrap.servers' = '10.1.30.8:9092',
    //        |      'format' = 'json',
    //        |      'scan.startup.mode' = 'latest-offset'
    //        |      )
    //        |""".stripMargin
    //
    //    tableEnv.executeSql(sink_table)
    //    tableEnv.executeSql("insert into source_table_2 select '1','2','3','4','5','6' from ICR_REWARD_DECLARE")
    // tableEnv.sqlQuery("select * from ICR_REWARD_DECLARE").toAppendStream[Row].print()
//    tableEnv.execute("haha")
    env.execute()
  }

  /**
   * 创建view的方法
   *
   * @param tableEnv
   * @param sql
   * @param tableName
   */
  def createView(tableEnv: StreamTableEnvironment, sql: String, tableName: String): Unit = {
    val table = tableEnv.sqlQuery(sql)
    tableEnv.createTemporaryView(tableName, table)
  }


}
