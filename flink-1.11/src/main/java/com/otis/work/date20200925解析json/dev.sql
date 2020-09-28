

select
PRH.PA01.PA01A.PA01AI01 as report_id,
PRH.PA01.PA01D.PA01DQ01 as fraud_cd,
PRH.PA01.PA01D.PA01DQ02 as fraud_tel,
PRH.PA01.PA01D.PA01DR01 as fraud_start_dt,
PRH.PA01.PA01D.PA01DR02 as fraud_end_dt,
cast(PRH.PA01.PA01E.PA01ES01 as bigint) as objection_num,
'2020-09-27'            as STATISTICS_DT
from ods_table;

select
PRH.PA01.PA01A.PA01AI01                 as report_id,
PIM.PB01.PB01A.PB01AD01                 as gender_cd,
PIM.PB01.PB01A.PB01AR01                 as birth_date,
PIM.PB01.PB01A.PB01AD02                 as edu_level_cd,
PIM.PB01.PB01A.PB01AD03                 as edu_degree_cd,
PIM.PB01.PB01A.PB01AD04                 as employment_cd,
PIM.PB01.PB01A.PB01AQ01                 as email,
PIM.PB01.PB01A.PB01AQ02                 as comm_addr,
PIM.PB01.PB01A.PB01AD05                 as nationality,
PIM.PB01.PB01A.PB01AQ03                 as reg_addr,
cast(PIM.PB01.PB01B.PB01AQ03 as bigint) as tel_cnt,
'2020-09-27'                            as STATISTICS_DT
from ods_table


select
t1.report_id                            as report_id,
info.PB01BQ01                           as tel_num,
info.PB01BR01                           as update_dt,
t1.SID                                  as SID,
t1.STATISTICS_DT                        as STATISTICS_DT
from(
SELECT
PRH.PA01.PA01A.PA01AI01                 as report_id,
PIM.PB01.PB01B.PB01BH                   as data,
PRH.PA01.PA01A.PA01AI01                 as SID,
'2020-09-27'                            as STATISTICS_DT
FROM ods_table)t1,unnest(t1.data) as cloumn(PB01BQ01,PB01BR01)




SELECT
PRH.PA01.PA01A.PA01AI01                 as report_id,
PMM.PB02.PB020D01                       as marital_stat_cd,
PMM.PB02.PB020Q01                       as spo_name,
PMM.PB02.PB020D02                       as spo_iden_cd,
PMM.PB02.PB020I01                       as spo_iden_id,
PMM.PB02.PB020Q02                       as spo_unit,
PMM.PB02.PB020Q03                       as spo_tel_num,
'2020-09-27'                            as STATISTICS_DT
FROM ods_table

PRM ROW(PB03 ARRAY<ROW(PB030D01 STRING,PB030Q01 STRING,PB030Q02 STRING,PB030R01 STRING)>)


select
t1.report_id                            as report_id,
info.PB030D01                           as res_cd,
info.PB030Q01                           as res_addr,
info.PB030Q02                           as res_tel,
info.PB030R01                           as res_update_dt,
t1.SID                                  as SID,
t1.STATISTICS_DT                        as STATISTICS_DT
from
(
select
PRH.PA01.PA01A.PA01AI01                 as report_id,
PRM.PB03                                as data,
PRH.PA01.PA01A.PA01AI01                 as SID,
'2020-09-27'                            as STATISTICS_DT
from ods_table
)t1,unnest(t1.data) as info(PB030D01,PB030Q01,PB030Q02,PB030R01)






select
t1.report_id                            as report_id,
info.PB040D01                           as work_situation,
info.PB040Q01                           as work_unit,
info.PB040D02                           as unit_property_cd,
info.PB040D03                           as industry_cd,
info.PB040Q02                           as unit_addr,
info.PB040Q03                           as unit_tel,
info.PB040D04                           as occupation_cd,
info.PB040D05                           as position_cd,
info.PB040D06                           as title_cd,
info.PB040R01                           as int_year,
info.PB040R02                           as pro_update_date,
t1.SID                                  as SID,
t1.STATISTICS_DT                        as STATISTICS_DT
from
(
select
PRH.PA01.PA01A.PA01AI01                 as report_id,
POM.PB04                                as data,
PRH.PA01.PA01A.PA01AI01                 as SID,
'2020-09-27'                            as STATISTICS_DT
from ods_table
)t1,unnest(t1.data) as info(PB040D01,PB040Q01,PB040D02,PB040D03,PB040Q02,PB040Q03,PB040D04,PB040D05,PB040D06,PB040R01,PB040R02)



select
PRH.PA01.PA01A.PA01AI01                 as report_id,
PSM.PC01.PC010Q01                       as score,
PSM.PC01.PC010Q02                       as score_level,
PSM.PC01.PC010S01                       as score_desc_num,
'2020-09-27'                            as STATISTICS_DT
from ods_table




select
PRH.PA01.PA01A.PA01AI01                 as report_id,
PSM.PC01.PC010D01                       as score_cd,
PRH.PA01.PA01A.PA01AI01                 as SID,
'2020-09-27'                            as STATISTICS_DT
from ods_table



select
PRH.PA01.PA01A.PA01AI01                 as report_id,
PCO.PC02.PC02A.PC02AS01                 as acct_total_cnt,
PCO.PC02.PC02A.PC02AS02                 as busi_type_num,
'2020-09-27'                            as STATISTICS_DT
from ods_table



select
t1.report_id                            as report_id,
info.PC02AD01                           as busi_type_cd,
info.PC02AD02                           as busi_kind_cd,
info.PC02AS03                           as acct_cnt,
info.PC02AR01                           as first_mon,
t1.SID                                  as SID,
t1.STATISTICS_DT                        as STATISTICS_DT
from
(select
PRH.PA01.PA01A.PA01AI01                 as report_id,
PCO.PC02.PC02A.PC02AH                   as data,
PRH.PA01.PA01A.PA01AI01                 as SID,
'2020-09-27'                            as STATISTICS_DT
from ods_table)t1,unest(t1.data) as info(PC02AD01,PC02AD02,PC02AS03,PC02AR01)



select
PRH.PA01.PA01A.PA01AI01                 as report_id,
PCO.PC02.PC02B.PC02BS01                 as rec_total_cnt,
PCO.PC02.PC02B.PC02BJ01                 as rec_total_bal,
PCO.PC02.PC02B.PC02BS02                 as rec_type_num,
'2020-09-27'                            as STATISTICS_DT
from ods_table




select
t1.report_id                            as report_id,
info.PC02BD01                           as rec_type_cd,
cast(info.PC02BS03 as bigint)           as rec_acct_cnt,
cast(info.PC02BJ02 as decimal(18,2))    as rec_bal,
t1.SID                                  as SID,
t1.STATISTICS_DT                        as STATISTICS_DT
from (
select
PRH.PA01.PA01A.PA01AI01                 as report_id,
PCO.PC02.PC02B.PC02BH                   as data,
PRH.PA01.PA01A.PA01AI01                 as SID,
'2020-09-27'                            as STATISTICS_DT
from ods_table
)t1,unnest(t1.data) as info(PC02BD01,PC02BS03,PC02BJ02)





select
PRH.PA01.PA01A.PA01AI01                         as report_id,
cast(PCO.PC02.PC02C.PC02CS01 as bigint)         as deadacct_cnt,
cast(PCO.PC02.PC02C.PC02CJ01 as decimal(18,2))  as deadacct_bal,
'2020-09-27'                                    as STATISTICS_DT
from ods_table




select
PRH.PA01.PA01A.PA01AI01                 as report_id,
cast(PCO.PC02D.PC02DS01 as bigint)      as ove_type_num,
'2020-09-27'                            as STATISTICS_DT
from ods_table

PCO ROW(PC02 ROW(
                  PC02A ROW(PC02AS01 STRING,PC02AS02 STRING,PC02AH ARRAY<ROW(PC02AD01 STRING,PC02AD02 STRING,PC02AS03 STRING,PC02AR01 STRING)>),
                  PC02B ROW(PC02BS01 STRING,PC02BJ01 STRING,PC02BS02 STRING,PC02BH ARRAY<ROW(PC02BD01 STRING,PC02BS03 STRING,PC02BJ02 STRING)>),
                  PC02C ROW(PC02CS01 STRING,PC02CJ01 STRING),
                  PC02D ROW(PC02DS01 STRING,PC02DH ARRAY<ROW(PC02DD01 STRING,PC02DS02 STRING,PC02DS03 STRING,PC02DJ01 STRING,PC02DS04 STRING)>),


select
t1.report_id                            as report_id,
info.PC02DD01                           as ove_type_cd,
info.PC02DS02                           as ove_acct_cnt,
info.PC02DS03                           as ove_mon_num,
info.PC02DJ01                           as ove_bal,
info.PC02DS04                           as ove_mon,
t1.SID                                  as SID,
t1.STATISTICS_DT                        as STATISTICS_DT
from(
select
PRH.PA01.PA01A.PA01AI01                 as report_id,
PCO.PC02.PC02D.PC02DH                   as data,
PRH.PA01.PA01A.PA01AI01                 as SID,
'2020-09-27'                            as STATISTICS_DT
from ods_table)t1,unnest(t1.data) as info(PC02DD01,PC02DS02,PC02DS03,PC02DJ01,PC02DS04)








-- todo ICR_QUERYREQ


select
PRH.PA01.PA01A.PA01AI01 as report_id,
PRH.PA01.PA01A.PA01AI01 as report_no,
PRH.PA01.PA01A.PA01AR01 as report_tm,
PRH.PA01.PA01B.PA01BQ01 as cust_name,
PRH.PA01.PA01B.PA01BD01 as query_iden_cd,
PRH.PA01.PA01B.PA01BI01 as query_iden_id,
PRH.PA01.PA01B.PA01BI02 as query_org_id,
PRH.PA01.PA01B.PA01BD02 as query_reason_cd,
'2020-09-27'            as STATISTICS_DT
from ods_table;

create table ICR_QUERYREQ(
PRH ROW(PA01 ROW(PA01B ROW(PA01BQ01 string,PA01BD01 string,PA01BI01 string,PA01BI02 string,PA01BD02 string),
                 PA01A ROW(PA01AI01 string,PA01AR01 string))),
report_id           AS PRH.PA01.PA01A.PA01AI01,
report_no           AS PRH.PA01.PA01A.PA01AI01,
report_tm           AS PRH.PA01.PA01A.PA01AR01,
cust_name		    AS PRH.PA01.PA01B.PA01BQ01,
query_iden_cd		AS PRH.PA01.PA01B.PA01BD01,
query_iden_id       AS PRH.PA01.PA01B.PA01BI01,
query_org_id        AS PRH.PA01.PA01B.PA01BI02,
query_reason_cd		AS PRH.PA01.PA01B.PA01BD02,
STATISTICS_DT	    AS '2020-09-27'
)with(
'connector' = 'filesystem',
'path' = 'file:///D:\\peoject\\Official-FlinkStudy\\flink-1.11\\src\\main\\java\\com\\otis\\work\\date20200925解析json\\c.json',
'format' = 'json'
)




CREATE TABLE ICR_QUERYREQ_print (
report_id        string,
report_no        string,
report_tm        string,
cust_name		 string,
query_iden_cd	string,
query_iden_id    string,
query_org_id     string,
query_reason_cd	string,
STATISTICS_DT	 string
) WITH (
'connector' = 'print')

-- todo ICR_OTHER_IDEN_NUM


create table ICR_OTHER_IDEN_NUM(
PRH ROW(PA01 ROW(PA01B ROW(PA01BQ01 string,PA01BD01 string,PA01BI01 string,PA01BI02 string,PA01BD02 string),
                 PA01A ROW(PA01AI01 string,PA01AR01 string),
                 PA01C ROW(PA01CS01 string))),
report_id           AS PRH.PA01.PA01A.PA01AI01,
iden_type_num       AS PRH.PA01.PA01C.PA01CS01,
STATISTICS_DT	    AS '2020-09-27'
)WITH(
'connector' = 'filesystem',
'path' = 'file:///D:\\peoject\\Official-FlinkStudy\\flink-1.11\\src\\main\\java\\com\\otis\\work\\date20200925解析json\\c.json',
'format' = 'json'
)

CREATE TABLE ICR_OTHER_IDEN_NUM_PRINT(
report_id           STRING,
iden_type_num       STRING,
STATISTICS_DT	    STRING
)WITH(
'connector' = 'print'
)

-- todo ICR_OTHER_IDEN

create table ICR_OTHER_IDEN(
PRH ROW(PA01 ROW(PA01B ROW(PA01BQ01 string,PA01BD01 string,PA01BI01 string,PA01BI02 string,PA01BD02 string),
                 PA01A ROW(PA01AI01 string,PA01AR01 string),
                 PA01C ROW(PA01CS01 string))),
report_id           AS PRH.PA01.PA01A.PA01AI01,

)WITH(
'connector' = 'filesystem',
'path' = 'file:///D:\\peoject\\Official-FlinkStudy\\flink-1.11\\src\\main\\java\\com\\otis\\work\\date20200925解析json\\c.json',
'format' = 'json'
)


iden_cd	varchar(2)		PA01CD01
iden_id	varchar(40)		PA01CI01
SID	varchar(25)	Y
STATISTICS_DT	string	Y






create table ods_table(
PRH ROW(PA01 ROW(
                  PA01B ROW(PA01BQ01 string,PA01BD01 string,PA01BI01 string,PA01BI02 string,PA01BD02 string),
                  PA01A ROW(PA01AI01 string,PA01AR01 string),
                  PA01C ROW(PA01CS01 string,PA01CH ARRAY<ROW(PA01CD01 string,PA01CI01 string)>),
                  PA01D ROW(PA01DQ01 string,PA01DQ02 string,PA01DR01 string,PA01DR02 string),
                  PA01E ROW(PA01ES01 STRING)
                 )),
-- PIM ROW(PB01 ROW(
--                   PB01A ROW(PB01AD01 SREING,PB01AR01 SREING,PB01AD02 SREING,PB01AD03 SREING,PB01AD04 SREING,PB01AQ01 SREING,PB01AQ02 SREING,PB01AD05 SREING,PB01AQ03 SREING),
--                   PB01B ROW(PB01BS01 STRING,PB01BH ARRAY<ROW(PB01BQ01 STRING,PB01BR01 STRING)>)
--         )),
-- PMM ROW(PB02 ROW(PB020D01 STRING,PB020Q01 STRING,PB020D02 STRING,PB020I01 STRING,PB020Q02 STRING,PB020Q03 STRING)),
-- PRM ROW(PB03 ARRAY<ROW(PB030D01 STRING,PB030Q01 STRING,PB030Q02 STRING,PB030R01 STRING)>),
-- POM ROW(PB04 ARRAY<ROW(PB040D01 STRING,PB040Q01 STRING,PB040D02 STRING,PB040D03 STRING,PB040Q02 STRING,PB040Q03 STRING,PB040D04 STRING,PB040D05 STRING,PB040D06 STRING,PB040R01 STRING,PB040R02 STRING)>),
-- PSM ROW(PC01 ROW(PC010Q01 STRING,PC010Q02 STRING,PC010S01 STRING,PC010D01 STRING)),
-- PCO ROW(PC02 ROW(
--                   PC02A ROW(PC02AS01 STRING,PC02AS02 STRING,PC02AH ARRAY<ROW(PC02AD01 STRING,PC02AD02 STRING,PC02AS03 STRING,PC02AR01 STRING)>),
--                   PC02B ROW(PC02BS01 STRING,PC02BJ01 STRING,PC02BS02 STRING,PC02BH ARRAY<ROW(PC02BD01 STRING,PC02BS03 STRING,PC02BJ02 STRING)>),
--                   PC02C ROW(PC02CS01 STRING,PC02CJ01 STRING),
--                   PC02D ROW(PC02DS01 STRING,PC02DH ARRAY<ROW(PC02DD01 STRING,PC02DS02 STRING,PC02DS03 STRING,PC02DJ01 STRING,PC02DS04 STRING)>),
--                   PC02E ROW(PC02ES01 STRING,PC02ES02 STRING,PC02EJ01 STRING,PC02EJ02 STRING,PC02EJ03 STRING),
--                   PC02F ROW(PC02FS01 STRING,PC02FS02 STRING,PC02FJ01 STRING,PC02FJ02 STRING,PC02FJ03 STRING),
--                   PC02G ROW(PC02GS01 STRING,PC02GS02 STRING,PC02GJ01 STRING,PC02GJ02 STRING,PC02GJ03 STRING),
--                   PC02H ROW(PC02HS01 STRING,PC02HS02 STRING,PC02HJ01 STRING,PC02HJ02 STRING,PC02HJ03 STRING,PC02HJ04 STRING,PC02HJ05 STRING),
--                   PC02I ROW(PC02IS01 STRING,PC02IS02 STRING,PC02IJ01 STRING,PC02IJ02 STRING,PC02IJ03 STRING,PC02IJ04 STRING,PC02IJ05 STRING),
--                   PC02K ROW(PC02KS01 STRING,PC02KH ARRAY<ROW(PC02KD01 STRING,PC02KD02 STRING,PC02KS02 STRING,PC02KJ01 STRING,PC02KJ02 STRING)>))
--       ),
-- PNO ROW(PC03 ROW(PC030S01 STRING,PC030H ARRAY<ROW(PC030D01 STRING,PC030S02 STRING,PC030J01 STRING)>)),
-- PPO ROW(PC04 ROW(PC040S01 STRING,PC040H ARRAY<ROW(PC040D01 STRING,PC040S02 STRING,PC040J01 STRING)>)),
-- PQO ROW(PC05 ROW(
--                     PC05A ROW(PC05AR01 STRING,PC05AD01 STRING,PC05AI01 STRING,PC05AQ01 STRING),
--                     PC05B ROW(PC05BS01 STRING,PC05BS02 STRING,PC05BS03 STRING,PC05BS04 STRING,PC05BS05 STRING,PC05BS06 STRING,PC05BS07 STRING,PC05BS08 STRING)
--     )),
-- PDA ROW(PD01 ARRAY<ROW(
--                     PD01A ROW(PD01AI01 STRING,PD01AD01 STRING,PD01AD02 STRING,PD01AI02 STRING,PD01AI03 STRING,PD01AI04 STRING,PD01AD03 STRING,PD01AR01 STRING,PD01AD04 STRING,PD01AJ01 STRING,PD01AJ02 STRING,PD01AJ03 STRING,PD01AR02 STRING,PD01AD05 STRING,PD01AD06 STRING,PD01AS01 STRING,PD01AD07 STRING,PD01AD08 STRING,PD01AD09 STRING,PD01AD10 STRING),
--                     PD01B ROW(PD01BD01 STRING,PD01BR01 STRING,PD01BR04 STRING,PD01BJ01 STRING,PD01BR02 STRING,PD01BJ02 STRING,PD01BD03 STRING,PD01BD04 STRING,PD01BR03 STRING),
--                     PD01C ROW(PD01CR01 STRING,PD01CD01 STRING,PD01CJ01 STRING,PD01CJ02 STRING,PD01CJ03 STRING,PD01CD02 STRING,PD01CS01 STRING,PD01CR02 STRING,PD01CJ04 STRING,PD01CJ05 STRING,PD01CR03 STRING,PD01CS02 STRING,PD01CJ06 STRING,PD01CJ07 STRING,PD01CJ08 STRING,PD01CJ09 STRING,PD01CJ10 STRING,PD01CJ11 STRING,PD01CJ12 STRING,PD01CJ13 STRING,PD01CJ14 STRING,PD01CJ15 STRING,PD01CR04 STRING),
--                     PD01D ROW(PD01DR01 STRING,PD01DR02 STRING,PD01DH ARRAY<ROW(PD01DR03 STRING,PD01DD01 STRING)>),
--                     PD01E ROW(PD01ER01 STRING,PD01ER02 STRING,PD01ES01 STRING,PD01EH ARRAY<ROW(PD01ER03 STRING,PD01ED01 STRING,PD01EJ01 STRING)>),
--                     PD01F ROW(PD01FS01 STRING,PD01FH ARRAY<ROW(PD01FD01 STRING,PD01FR01 STRING,PD01FS02 STRING,PD01FJ01 STRING,PD01FQ01 STRING)>),
--                     PD01G ROW(PD01GS01 STRING,PD01GH ARRAY<ROW(PD01GR01 STRING,PD01GD01 STRING)>),
--                     PD01H ROW(PD01HS01 STRING,PD01HH ARRAY<ROW(PD01HJ01 STRING,PD01HR01 STRING,PD01HR02 STRING,PD01HJ02 STRING)>),
--                     PD01Z ROW(PD01ZS01 STRING,PD01ZH ARRAY<ROW(PD01ZD01 STRING,PD01ZQ01 STRING,PD01ZR01 STRING)>)
--       )>),
-- PCA ROW(PD02 ARRAY<ROW(
--                         PD02A ROW(PD02AI01 STRING,PD02AD01 STRING,PD02AI02 STRING,PD02AI03 STRING,PD02AD02 STRING,PD02AJ01 STRING,PD02AD03 STRING,PD02AR01 STRING,PD02AR02 STRING,PD02AD04 STRING,PD02AJ04 STRING,PD02AJ03 STRING,PD02AI04 STRING),
--                         PD02Z ROW(PD02ZS01 STRING,PD02ZH ARRAY<ROW(PD02ZD01 STRING,PD02ZQ01 STRING,PD02ZR01 STRING)>)
--                         )>),
-- PCR ROW(PD03 ARRAY<ROW(
--                         PD03A ROW(PD03AD08 STRING,PD03AD01 STRING,PD03AQ01 STRING,PD03AD02 STRING,PD03AR01 STRING,PD03AR02 STRING,PD03AD03 STRING,PD03AQ02 STRING,PD03AJ01 STRING,PD03AD04 STRING,PD03AJ02 STRING,PD03AD05 STRING,PD03AD06 STRING,PD03AD07 STRING,PD03AS01 STRING,PD03AR03 STRING),
--                         PD03Z ROW(PD03ZS01 STRING,PD03ZH ARRAY<PD03ZD01 STRING>)
--                       >)),
-- PND ROW(PE01 ARRAY<ROW(
--                         PE01A ROW(PE01AD01 STRING,PE01AQ01 STRING,PE01AD02 STRING,PE01AR01 STRING,PE01AD03 STRING,PE01AJ01 STRING,PE01AR02 STRING,PE01AQ02 STRING),
--                         PE01Z ROW(PE01ZS01 STRING,PE01ZH ARRAY<ROW(PE01ZD01 STRING,PE01ZQ01 STRING,PE01ZR01 STRING)>)
--                 )>),
-- POT ROW(PF01 ARRAY<ROW(
--                         PF01A ROW(PF01AQ01 STRING,PF01AJ01 STRING,PF01AR01 STRING),
--                         PF01Z ROW(PF01ZS01 STRING,PF01ZH ARRAY<ROW(PF01ZD01 STRING,PF01ZQ01 STRING,PF01ZR01 STRING)>))
--                 >),
-- PCJ ROW(PF02 ARRAY<ROW(
--                         PF02A ROW(PF02AQ01 STRING,PF02AQ02 STRING,PF02AR01 STRING,PF02AD01 STRING,PF02AQ03 STRING,PF02AR02 STRING,PF02AQ04 STRING,PF02AJ01 STRING),
--                         PF02Z ROW(PF02ZS01 STRING,PF02ZH ARRAY<ROW(PF02ZD01 STRING,PF02ZQ01 STRING,PF02ZR01 STRING)>)
--                 )>),
-- PCE ROW(PF03 ARRAY<ROW(
--                         PF03A ROW(PF03AQ01 STRING,PF03AQ02 STRING,PF03AR01 STRING,PF03AD01 STRING,PF03AQ03 STRING,PF03AR02 STRING,PF03AQ04 STRING,PF03AJ01 STRING,PF03AQ05 STRING,PF03AJ02 STRING),
--                         PF03Z ROW(PF03ZS01 STRING,PF03ZH ARRAY<ROW(PF03ZD01 STRING,PF03ZQ01 STRING,PF03ZR01 STRING)>)
--                 )>),
-- PAP ROW(PF04 ARRAY<ROW(
--                         PF04A ROW(PF04AQ01 STRING,PF04AQ02 STRING,PF04AJ01 STRING,PF04AR01 STRING,PF04AR02 STRING,PF04AQ03 STRING),
--                         PF04Z ROW(PF04ZS01 STRING,PF04ZH ARRAY<ROW(PF04ZD01 STRING,PF04ZQ01 STRING,PF04ZR01 STRING)>)
--                 )>),
-- PHF ROW(PF05 ARRAY<ROW(
--                         PF05A ROW(PF05AQ01 STRING,PF05AR01 STRING,PF05AD01 STRING,PF05AR02 STRING,PF05AR03 STRING,PF05AQ02 STRING,PF05AQ03 STRING,PF05AJ01 STRING,PF05AQ04 STRING,PF05AR04 STRING),
--                         PF05Z ROW(PF05ZS01 STRING,PF05ZH ARRAY<ROW(PF05ZD01 STRING,PF05ZQ01 STRING,PF05ZR01 STRING)>)
--                 )>),
-- PBS ROW(PF06 ARRAY<ROW(
--                         PF06A ROW(PF06AD01 STRING,PF06AQ01 STRING,PF06AQ02 STRING,PF06AQ03 STRING,PF06AR01 STRING,PF06AR02 STRING,PF06AR03 STRING),
--                         PF06Z ROW(PF06ZS01 STRING,PF06ZH ARRAY<ROW(PF06ZD01 STRING,PF06ZQ01 STRING,PF06ZR01 STRING)>)
--                 )>),
-- PPQ ROW(PF07 ARRAY<ROW(
--                         PF07A ROW(PF07AQ01 STRING,PF07AQ02 STRING,PF07AD01 STRING,PF07AD02 STRING,PF07AR01 STRING,PF07AR02 STRING,PF07AR03 STRING),
--                         PF07Z ROW(PF07ZS01 STRING,PF07ZH ARRAY<ROW(PF07ZD01 STRING,PF07ZQ01 STRING,PF07ZR01 STRING)>)
--                 )>),
-- PAH ROW(PF08 ARRAY<ROW(
--                         PF08A ROW(PF08AQ01 STRING,PF08AQ02 STRING,PF08AR01 STRING,PF08AR02 STRING),
--                         PF08Z ROW(PF08ZS01 STRING,PF08ZH ARRAY<ROW(PF08ZD01 STRING,PF08ZQ01 STRING,PF08ZR01 STRING)>)
--                 )>),
-- POS ROW(PG01 ARRAY<ROW(PG010D01 STRING,PG010D02 STRING)>),
-- POQ ROW(PH01 ARRAY<ROW(PH010R01 STRING,PH010D01 STRING,PH010Q02 STRING,PH010Q03 STRING)>)
)WITH(
'connector' = 'filesystem',
'path' = 'file:///D:\\peoject\\Official-FlinkStudy\\flink-1.11\\src\\main\\java\\com\\otis\\work\\date20200925解析json\\c.json',
'format' = 'json'
)













