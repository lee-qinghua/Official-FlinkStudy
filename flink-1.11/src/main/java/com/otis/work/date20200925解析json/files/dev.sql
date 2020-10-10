

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
PRH.PA01.PA01A.PA01AI01                 as report_id,
PCO.PC02.PC02E.PC02ES02                 as oneoff_acct_cnt,
PCO.PC02.PC02E.PC02EJ01                 as oneoff_credit_amt,
PCO.PC02.PC02E.PC02EJ02                 as oneoff_bal,
PCO.PC02.PC02E.PC02EJ03                 as oneoff_6mon_avg,
'2020-09-27'                            as STATISTICS_DT
from ods_table



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
info.xx1 as field1,
info.xx2 as field2
from(
select
A.B.C   as data
from ods_table
)t1,unnest(t1.data) as info(xx1,xx2)




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


select
t1.report_id                            as report_id,
info.PC02KD01                           as rep_iden_type,
info.PC02KD02                           as rep_duty_type_cd,
cast(info.PC02KS02 as bigint)           as rep_acct_cnt,
cast(info.PC02KJ01 as decimal(18,2))    as rep_amt,
cast(info.PC02KJ02 as decimal(18,2))    as rep_bal,
t1.SID                                  as SID,
t1.STATISTICS_DT                        as STATISTICS_DT
from(
select
PRH.PA01.PA01A.PA01AI01  				as report_id,
PCO.PC02.PC02K.PC02KH         as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
from ods_table
)t1,unnest(t1.data) as info(PC02KD01,PC02KD02,PC02KS02,PC02KJ01,PC02KJ02)



select
t1.report_id                            as report_id,
info.PC030D01                           as    pos_type_cd,
cast(info.PC030S02 as bigint)           as    pos_acct_cnt,
cast(info.PC030J01 as decimal(18,2))    as    pos_bal,
t1.SID                                  as SID,
t1.STATISTICS_DT                        as STATISTICS_DT
(
select
PRH.PA01.PA01A.PA01AI01  				as report_id,
PNO.PC030H         as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
from ods_table
)t1,unnest(t1.data) as info(PC030D01,PC030S02,PC030J01)


select
t1.report_id                            as report_id,
info.PC040D01                           as pub_type_cd,
cast(info.PC040S02 as bigint)           as pub_cnt,
cast(info.PC040J01 as decimal(18,2))    as pub_amt,
t1.SID                                  as SID,
t1.STATISTICS_DT                        as STATISTICS_DT
from(
select
PRH.PA01.PA01A.PA01AI01  				as report_id,
PPO.PC040H         as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
from ods_table
)t1,unnest(t1.data) as info(PC040D01,PC040S02,PC040J01)


select
t1.report_id                            as report_id,
info.PH010R01                           as que_dt,
info.PH010D01                           as que_org_type,
info.PH010Q02                           as que_org_id,
info.PH010Q03                           as que_reason,
t1.SID                                  as SID,
t1.STATISTICS_DT                        as STATISTICS_DT
from (
select
PRH.PA01.PA01A.PA01AI01  				as report_id,
POQ.PH01         as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
from ods_table
)t1,unnest(t1.data) as info(PH010R01,PH010D01,PH010Q02,PH010Q03)



select
t1.report_id                            as report_id,
info.PD01A.PD01AI01                     as acct_num,
info.PD01A.PD01AD01                     as acct_type_cd,
info.PD01A.PD01AD02                     as acct_org_type_cd,
info.PD01A.PD01AI02                     as acct_org_id,
info.PD01A.PD01AI03                     as acct_sign,
info.PD01A.PD01AI04                     as acct_agmt_num,
info.PD01A.PD01AD03                     as busi_spec_cd,
info.PD01A.PD01AR01                     as open_dt,
info.PD01A.PD01AD04                     as currency_cd,
cast(info.PD01A.PD01AJ01 as decimal(18,2)) as due_amt,
cast(info.PD01A.PD01AJ02 as decimal(18,2)) as credit_amt,
cast(info.PD01A.PD01AJ03 as decimal(18,2)) as credit_amt_share,
info.PD01A.PD01AR02                     as due_dt,
info.PD01A.PD01AD05                     as repay_mode_cd,
info.PD01A.PD01AD06                     as repay_frequency_cd,
cast(info.PD01A.PD01AS01 as bigint)     as repay_period,
info.PD01A.PD01AD07                     as grt_mode_cd,
info.PD01A.PD01AD08                     as lending_mode_cd,
info.PD01A.PD01AD09                     as share_sign_cd,
t1.SID                                  as SID,
t1.STATISTICS_DT                        as STATISTICS_DT
from(
select
PRH.PA01.PA01A.PA01AI01  				as report_id,
PDA.PD01                                as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
from ods_table
)t1,unnest(t1.data) as info(PD01A,PD01B,PD01C,PD01D,PD01E,PD01F,PD01G,PD01H,PD01Z)



select
t1.report_id                            as report_id,
info.PD01A.PD01AI01                     as acct_num,
info.PD01B.PD01BD01                     as acct_stat,
info.PD01B.PD01BR01                     as close_dt,
info.PD01B.PD01BR04                     as out_month,
cast(info.PD01B.PD01BJ01 as decimal(18,2))  as loan_bal,
info.PD01B.PD01BR02                     as latest_repay_dt,
cast(info.PD01B.PD01BJ02 as decimal(18,2)) as latest_repay_amt,
info.PD01B.PD01BD03                     as five_class_cd,
info.PD01B.PD01BD04                     as repay_stat,
info.PD01B.PD01BR03                     as report_dt,
t1.SID                                  as SID,
t1.STATISTICS_DT                        as STATISTICS_DT
from(
select
PRH.PA01.PA01A.PA01AI01  				as report_id,
PDA.PD01                                as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
from ods_table
)t1,unnest(t1.data) as info(PD01A,PD01B,PD01C,PD01D,PD01E,PD01F,PD01G,PD01H,PD01Z)


PDA ROW(PD01 ARRAY<ROW(
                    PD01A ROW(PD01AI01 STRING,PD01AD01 STRING,PD01AD02 STRING,PD01AI02 STRING,PD01AI03 STRING,PD01AI04 STRING,PD01AD03 STRING,PD01AR01 STRING,PD01AD04 STRING,PD01AJ01 STRING,PD01AJ02 STRING,PD01AJ03 STRING,PD01AR02 STRING,PD01AD05 STRING,PD01AD06 STRING,PD01AS01 STRING,PD01AD07 STRING,PD01AD08 STRING,PD01AD09 STRING,PD01AD10 STRING),
                    PD01B ROW(PD01BD01 STRING,PD01BR01 STRING,PD01BR04 STRING,PD01BJ01 STRING,PD01BR02 STRING,PD01BJ02 STRING,PD01BD03 STRING,PD01BD04 STRING,PD01BR03 STRING),
                    PD01C ROW(PD01CR01 STRING,PD01CD01 STRING,PD01CJ01 STRING,PD01CJ02 STRING,PD01CJ03 STRING,PD01CD02 STRING,PD01CS01 STRING,PD01CR02 STRING,PD01CJ04 STRING,PD01CJ05 STRING,PD01CR03 STRING,PD01CS02 STRING,PD01CJ06 STRING,PD01CJ07 STRING,PD01CJ08 STRING,PD01CJ09 STRING,PD01CJ10 STRING,PD01CJ11 STRING,PD01CJ12 STRING,PD01CJ13 STRING,PD01CJ14 STRING,PD01CJ15 STRING,PD01CR04 STRING),
                    PD01D ROW(PD01DR01 STRING,PD01DR02 STRING,PD01DH ARRAY<ROW(PD01DR03 STRING,PD01DD01 STRING)>),
                    PD01E ROW(PD01ER01 STRING,PD01ER02 STRING,PD01ES01 STRING,PD01EH ARRAY<ROW(PD01ER03 STRING,PD01ED01 STRING,PD01EJ01 STRING)>),
                    PD01F ROW(PD01FS01 STRING,PD01FH ARRAY<ROW(PD01FD01 STRING,PD01FR01 STRING,PD01FS02 STRING,PD01FJ01 STRING,PD01FQ01 STRING)>),
                    PD01G ROW(PD01GS01 STRING,PD01GH ARRAY<ROW(PD01GR01 STRING,PD01GD01 STRING)>),
                    PD01H ROW(PD01HS01 STRING,PD01HH ARRAY<ROW(PD01HJ01 STRING,PD01HR01 STRING,PD01HR02 STRING,PD01HJ02 STRING)>),
                    PD01Z ROW(PD01ZS01 STRING,PD01ZH ARRAY<ROW(PD01ZD01 STRING,PD01ZQ01 STRING,PD01ZR01 STRING)>)
      )


select
t1.report_id                                as report_id,
info.PD01A.PD01AI01                         as acct_num,
info.PD01C.PD01CR01                         as mmonth,
info.PD01C.PD01CD01                         as acct_stat_1m,
cast(info.PD01C.PD01CJ01 as decimal(18,2))  as loan_bal_1m,
cast(info.PD01C.PD01CJ02 as decimal(18,2))  as credit_amt_used,
cast(info.PD01C.PD01CJ03 as decimal(18,2))  as credit_amt_big,
info.PD01C.PD01CD02                         as five_class_1m,
cast(info.PD01C.PD01CS01 as bigint)         as repay_period_left,
info.PD01C.PD01CR02                         as repay_date,
cast(info.PD01C.PD01CJ04 as bigint)         as repay_amt,
cast(info.PD01C.PD01CJ05 as bigint)         as repay_amt_act,
info.PD01C.PD01CR03                         as repay_date_latest,
cast(info.PD01C.PD01CS02 as bigint)         as overdue_period,
cast(info.PD01C.PD01CJ06 as decimal(18,2))  as overdue_amt,
cast(info.PD01C.PD01CJ07 as decimal(18,2))  as overdue_prin_31Amt,
cast(info.PD01C.PD01CJ08 as decimal(18,2))  as overdue_prin_61Amt,
cast(info.PD01C.PD01CJ09 as decimal(18,2))  as overdue_prin_91Amt,
cast(info.PD01C.PD01CJ10 as decimal(18,2))  as overdue_prin_180Amt,
cast(info.PD01C.PD01CJ11 as decimal(18,2))  as overdraft_prin_180Amt,
cast(info.PD01C.PD01CJ12 as decimal(18,2))  as credit_6mon_avg,
cast(info.PD01C.PD01CJ13 as decimal(18,2))  as overdraft_6mon_avg,
cast(info.PD01C.PD01CJ14 as decimal(18,2))  as max_credit_used,
cast(info.PD01C.PD01CJ15 as decimal(18,2))  as max_overdraft,
info.PD01C.PD01CR04                         as report_dt_1mon,
t1.SID                                  as SID,
t1.STATISTICS_DT                        as STATISTICS_DT
from(
select
PRH.PA01.PA01A.PA01AI01  				as report_id,
PDA.PD01                                as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
from ods_table
)t1,unnest(t1.data) as info(PD01A,PD01B,PD01C,PD01D,PD01E,PD01F,PD01G,PD01H,PD01Z)



select
t1.report_id                                as report_id,
info.PD01A.PD01AI01                         as acct_num,
info.PD01D.PD01DR01                         as start_dt,
info.PD01D.PD01DR02                         as end_dt,
t1.SID                                      as SID,
t1.STATISTICS_DT                            as STATISTICS_DT
from(
select
PRH.PA01.PA01A.PA01AI01  				as report_id,
PDA.PD01                                as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
from ods_table
)t1,unnest(t1.data) as info(PD01A,PD01B,PD01C,PD01D,PD01E,PD01F,PD01G,PD01H,PD01Z)



select
t1.report_id as report_id,
t1.SID as SID,
t1.STATISTICS_DT as STATISTICS_DT,
info.PD01A  as PD01A,
info.PD01B  as PD01B,
info.PD01C  as PD01C,
info.PD01D  as PD01D,
info.PD01E  as PD01E,
info.PD01F  as PD01F,
info.PD01G  as PD01G,
info.PD01H  as PD01H,
info.PD01Z  as PD01Z
from(
select
PDA.PD01   as data,
PRH.PA01.PA01A.PA01AI01  				as report_id,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
from ods_table
)t1,unnest(t1.data) as info(PD01A,PD01B,PD01C,PD01D,PD01E,PD01F,PD01G,PD01H,PD01Z)


select
t1.report_id                                    as report_id,
t1.acct_num                                     as acct_num,
info.PD01DR03                                   as month_24m,
info.PD01DD01                                   as repay_stat_24m,
t1.SID  				                        as SID,
t1.STATISTICS_DT                                as STATISTICS_DT
from(
select
PD01A.PD01AI01 as acct_num,
report_id as report_id,
SID as SID,
STATISTICS_DT as  STATISTICS_DT,
PD01D.PD01DH as data
from PDAPD01
)t1,unnest(t1.data) as info(PD01DR03,PD01DD01)







select
report_id as report_id,
PD01A.PD01AI01 as acct_num,
PD01E.PD01ER01 as start_dt_5y,
PD01E.PD01ER02 as end_dt_5y,
cast(PD01E.PD01ES01 as bigint) as mon_num,
SID as SID,
STATISTICS_DT as STATISTICS_DT
from PDAPD01





select
t1.report_id                                    as report_id,
t1.acct_num                                     as acct_num,
info.PD01ER03                                   as month_5y,
info.PD01ED01                                   as repay_stat_5y,
cast(info.PD01EJ01 as decimal(18,2))            as amt_5y,
t1.SID  				                        as SID,
t1.STATISTICS_DT                                as STATISTICS_DT
from(
select
PD01A.PD01AI01      as acct_num,
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PD01E.PD01EH        as data
from PDAPD01
)t1,unnest(t1.data) as info(PD01ER03,PD01ED01,PD01EJ01)


select
report_id as report_id,
PD01A.PD01AI01      as acct_num,
cast(PD01F.PD01FS01 as bigint) as spe_txn_num,
SID as SID,
STATISTICS_DT as STATISTICS_DT
from PDAPD01



select
t1.report_id as report_id,
t1.acct_num as acct_num,
info.PD01FD01 as spe_txn_type_cd,
info.PD01FR01 as spe_txn_dt,
cast(info.PD01FS02 as bigint) as spe_end_dt_change,
cast(info.PD01FJ01 as decimal(18,2)) as spe_amt,
info.PD01FQ01 as spe_record,
t1.SID as SID,
t1.STATISTICS_DT as STATISTICS_DT
from(
select
PD01A.PD01AI01      as acct_num,
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PD01F.PD01FH     as data
from PDAPD01
)t1,unnest(t1.data) as info(PD01FD01,PD01FR01,PD01FS02,PD01FJ01,PD01FQ01)


select
report_id as report_id,
PD01A.PD01AI01      as acct_num,
cast(PD01G.PD01GS01 as bigint) as spe_event_num,
SID as SID,
STATISTICS_DT as STATISTICS_DT
from PDAPD01


select
t1.report_id as report_id,
t1.acct_num as acct_num,
info.PD01GR01 as spe_event_dt,
info.PD01GD01 as spe_event_type,
t1.SID as SID,
t1.STATISTICS_DT as STATISTICS_DT
from(
select
report_id 			as report_id,
PD01A.PD01AI01      as acct_num,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PD01G.PD01GH     as data
from PDAPD01
)t1,unnest(t1.data) as info(PD01GR01,PD01GD01)



select
report_id as report_id,
PD01A.PD01AI01      as acct_num,
cast(PD01H.PD01HS01 as bigint) as large_cnt,
SID as SID,
STATISTICS_DT as STATISTICS_DT
from PDAPD01





select
t1.report_id as report_id,
t1.acct_num as acct_num,
cast(info.PD01HJ01 as decimal(18,2)) as large_amt,
info.PD01HR01 as large_eff_dt,
info.PD01HR02 as large_end_dt,
cast(info.PD01HJ02 as decimal(18,2)) as large_amt_used,
t1.SID as SID,
t1.STATISTICS_DT as STATISTICS_DT
from(
select
report_id 			as report_id,
PD01A.PD01AI01      as acct_num,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PD01H.PD01HH     as data
from PDAPD01
)t1,unnest(t1.data) as info(PD01HJ01,PD01HR01,PD01HR02,PD01HJ02)


select
report_id as report_id,
PD01A.PD01AI01      as acct_num,
cast(PD01Z.PD01ZS01 as bgiint) as declare_num,
SID as SID,
STATISTICS_DT as STATISTICS_DT
from PDAPD01



select
t1.report_id as report_id,
t1.acct_num as acct_num,
info.PD01ZD01 as declare_type_cd,
info.PD01ZQ01 as declare_content,
info.PD01ZR01 as declare_dt,
t1.SID as SID,
t1.STATISTICS_DT as STATISTICS_DT
from(
select
report_id 			as report_id,
PD01A.PD01AI01      as acct_num,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PD01Z.PD01ZH     as data
from PDAPD01
)t1,unnest(t1.data) as info(PD01ZD01,PD01ZQ01,PD01ZR01)





select
report_id as report_id,
PD02A.PD02AI01 as agmt_num,
cast(PD02Z.PD02ZS01 as bigint) as agmt_dec_num,
SID as SID,
STATISTICS_DT as STATISTICS_DT
from PCAPD02


select
report_id as report_id,
PD02A.PD02AI01 as agmt_num,
PD02A.PD02AD01  as agmt_org_type,
PD02A.PD02AI02  as agmt_org_id,
PD02A.PD02AI03  as agmt_sign,
PD02A.PD02AD02  as agmt_use_cd,
cast(PD02A.PD02AJ01 as decimal(18,2))  as agmt_amt,
PD02A.PD02AD03  as agmt_currency_cd,
PD02A.PD02AR01  as agmt_eff_dt,
PD02A.PD02AR02  as agmt_due_dt,
PD02A.PD02AD04  as agmt_stat_cd,
cast(PD02A.PD02AJ04 as decimal(18,2))  as agmt_amt_used,
PD02A.PD02AI04  as agmt_limit_id,
cast(PD02A.PD02AJ03 as decimal(18,2))  as agmt_limit,
SID as SID,
STATISTICS_DT as STATISTICS_DT
from PCAPD02



select
t1.report_id as report_id,
t1.agmt_num as agmt_num,
info.PD02ZD01 as agmt_decl_type,
info.PD02ZQ01 as agmt_decl_cont,
info.PD02ZR01 as agmt_decl_dt,
t1.SID as SID,
t1.STATISTICS_DT as STATISTICS_DT
from(
select
report_id 			as report_id,
PD02A.PD02AI01      as agmt_num,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PD02Z.PD02ZH     as data
from PCAPD02
)t1,unnest(t1.data) as info(PD02ZD01,PD02ZQ01,PD02ZR01)


select
report_id as report_id,
PE01.PE01AD01 as postpaid_acct_type_cd,
PE01.PE01AQ01 as postpaid_org_name,
PE01.PE01AD02 as postpaid_busi_type_cd,
PE01.PE01AR01 as postpaid_open_dt,
PE01.PE01AD03 as postpaid_stat_cd,
cast(PE01.PE01AJ01 as decimal(18,2)) as postpaid_owe_amt,
PE01.PE01AR02 as postpaid_acct_mon,
PE01.PE01AQ02 as postpaid_24mon,
STATISTICS_DT as STATISTICS_DT,
SID as SID
from PNDPE01



select
report_id as report_id,
PF01A.PF01AQ01 as tax_org_id,
cast(PF01A.PF01AJ01 as decimal(18,2)) as tax_owe_amt,
PF01A.PF01AR01 as tax_owe_dt,
STATISTICS_DT as STATISTICS_DT
from POTPF01



select
report_id as report_id,
cast(PF01Z.PF01ZS01 as bigint) as tax_owe_amt,
STATISTICS_DT as STATISTICS_DT
from POTPF01


select
report_id as report_id,
PF02A.PF02AQ01 as civ_court_id,
PF02A.PF02AQ02 as civ_reason,
PF02A.PF02AR01 as civ_reg_dt,
PF02A.PF02AD01 as civ_closed_cd,
PF02A.PF02AQ03 as civ_result,
PF02A.PF02AR02 as civ_eff_dt,
PF02A.PF02AQ04 as civ_target,
cast(PF02A.PF02AJ01 as decimal(18,2)) as civ_amt,
STATISTICS_DT as STATISTICS_DT,
SID as SID
from PCJPF02


select
report_id as report_id,
cast(PF02Z.PF02ZS01 as bigint) as civ_decl_num,
STATISTICS_DT as STATISTICS_DT
from PCJPF02



select
report_id as report_id,
PF03A.PF03AQ01 as   enf_court_id,
PF03A.PF03AQ02 as   enf_reason,
PF03A.PF03AR01 as   enf_reg_dt,
PF03A.PF03AD01 as   enf_closed_cd,
PF03A.PF03AQ03 as   enf_stat,
PF03A.PF03AR02 as   enf_closed_dt,
PF03A.PF03AQ04 as   enf_apply_target,
cast(PF03A.PF03AJ01 as decimal(18,2)) as   enf_apply_amt,
PF03A.PF03AQ05 as   enf_execute_target,
cast(PF03A.PF03AJ02 as decimal(18,2)) as   enf_execute_amt,
STATISTICS_DT as STATISTICS_DT,
SID as SID
from PCEPF03




select
report_id as report_id,
cast(PF03Z.PF03ZS01 as decimal(18,2)) as enf_decl_num,
STATISTICS_DT as STATISTICS_DT
from PCEPF03

PAP ROW(PF04 ARRAY<ROW(
                        PF04A ROW(PF04AQ01 STRING,PF04AQ02 STRING,PF04AJ01 STRING,PF04AR01 STRING,PF04AR02 STRING,PF04AQ03 STRING),
                        PF04Z ROW(PF04ZS01 STRING,PF04ZH ARRAY<ROW(PF04ZD01 STRING,PF04ZQ01 STRING,PF04ZR01 STRING)>)
                )>)

select
report_id as report_id,
PF04A.PF04AQ01 as punish_org_id,
PF04A.PF04AQ02 as punish_cont,
cast(PF04A.PF04AJ01 as decimal(18,2)) as punish_amt,
PF04A.PF04AR01 as punish_eff_dt,
PF04A.PF04AR02 as punish_end_dt,
PF04A.PF04AQ03 as punish_result,
STATISTICS_DT as STATISTICS_DT,
SID as SID
from PAPPF04



select
report_id as report_id,
cast(PF04Z.PF04ZS01 as bigint) as punish_decl_num,
STATISTICS_DT as STATISTICS_DT
from PAPPF04


PHF ROW(PF05 ARRAY<ROW(
                        PF05A ROW(PF05AQ01 STRING,PF05AR01 STRING,PF05AD01 STRING,PF05AR02 STRING,PF05AR03 STRING,PF05AQ02 STRING,PF05AQ03 STRING,PF05AJ01 STRING,PF05AQ04 STRING,PF05AR04 STRING),
                        PF05Z ROW(PF05ZS01 STRING,PF05ZH ARRAY<ROW(PF05ZD01 STRING,PF05ZQ01 STRING,PF05ZR01 STRING)>)
                )>),

select
report_id as report_id,
PF05A.PF05AQ01 as housing_area,
PF05A.PF05AR01 as housing_dt,
PF05A.PF05AD01 as housing_stat_cd,
PF05A.PF05AR02 as housing_start_dt,
PF05A.PF05AR03 as housing_last_dt,
cast(PF05A.PF05AQ02 as bigint) as housing_prop,
cast(PF05A.PF05AQ03 as bigint) as housing_prop_indiv,
cast(PF05A.PF05AJ01 as bigint) as housing_amt,
PF05A.PF05AQ04 as housing_unit,
PF05A.PF05AR04 as housing_update_dt,
STATISTICS_DT as STATISTICS_DT,
SID as SID
from PHFPF05

select
report_id as report_id,
cast(PF05Z.PF05ZS01 as bigint) as housing_decl_num,
STATISTICS_DT as STATISTICS_DT
from PHFPF05




PBS ROW(PF06 ARRAY<ROW(
                        PF06A ROW(PF06AD01 STRING,PF06AQ01 STRING,PF06AQ02 STRING,PF06AQ03 STRING,PF06AR01 STRING,PF06AR02 STRING,PF06AR03 STRING),
                        PF06Z ROW(PF06ZS01 STRING,PF06ZH ARRAY<ROW(PF06ZD01 STRING,PF06ZQ01 STRING,PF06ZR01 STRING)>)
                )>),


select
report_id as report_id,
PF06A.PF06AD01 as   allowance_type_cd,
PF06A.PF06AQ01 as   allowance_area,
PF06A.PF06AQ02 as   allowance_unit,
cast(PF06A.PF06AQ03 as bigint) as   allowance_income,
PF06A.PF06AR01 as   allowance_sup_dt,
PF06A.PF06AR02 as   allowance_app_dt,
PF06A.PF06AR03 as   allowance_update_dt,
STATISTICS_DT as STATISTICS_DT,
SID as SID
from PBSPF06



select
report_id as report_id,
cast(PF06Z.PF06ZS01 as bigint) as allowance_decl_num,
STATISTICS_DT as STATISTICS_DT
from PBSPF06


PPQ ROW(PF07 ARRAY<ROW(
                        PF07A ROW(PF07AQ01 STRING,PF07AQ02 STRING,PF07AD01 STRING,PF07AD02 STRING,PF07AR01 STRING,PF07AR02 STRING,PF07AR03 STRING),
                        PF07Z ROW(PF07ZS01 STRING,PF07ZH ARRAY<ROW(PF07ZD01 STRING,PF07ZQ01 STRING,PF07ZR01 STRING)>)
                )>),

select
report_id      as report_id,
PF07A.PF07AQ01 as   qual_name,
PF07A.PF07AQ02 as   qual_org,
PF07A.PF07AD01 as   qual_level_cd,
PF07A.PF07AD02 as   qual_area,
PF07A.PF07AR01 as   qual_get_dt,
PF07A.PF07AR02 as   qual_due_dt,
PF07A.PF07AR03 as   qual_revoke_dt,
STATISTICS_DT as STATISTICS_DT,
SID as SID
from PPQPF07

select
report_id as report_id,
cast(PF07Z.PF07ZS01 as bigint) as allowance_decl_num,
STATISTICS_DT as STATISTICS_DT
from PPQPF07



PAH ROW(PF08 ARRAY<ROW(
                        PF08A ROW(PF08AQ01 STRING,PF08AQ02 STRING,PF08AR01 STRING,PF08AR02 STRING),
                        PF08Z ROW(PF08ZS01 STRING,PF08ZH ARRAY<ROW(PF08ZD01 STRING,PF08ZQ01 STRING,PF08ZR01 STRING)>)
                )>),


select
report_id      as   report_id,
PF08A.PF08AQ01 as   rew_org,
PF08A.PF08AQ02 as   rew_cont,
PF08A.PF08AR01 as   rew_eff_dt,
PF08A.PF08AR02 as   rew_end_dt,
STATISTICS_DT as STATISTICS_DT,
SID as SID
from PAHPF08


select
report_id as report_id,
cast(PF08Z.PF08ZS01 as bigint) as allowance_decl_num,
STATISTICS_DT as STATISTICS_DT
from PAHPF08





select
t1.report_id  as  report_id,
info.PE01ZD01 as  postpaid_decl_type,
info.PE01ZQ01 as  postpaid_decl_cont,
info.PE01ZR01 as  postpaid_decl_dt,
t1.SID as SID,
t1.STATISTICS_DT as STATISTICS_DT
from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PE01Z.PE01ZH     as data
from PNDPE01
)t1,unnest(t1.data) as info(PE01ZD01,PE01ZQ01,PE01ZR01)






select
t1.report_id  as  report_id,
info.PF01ZD01 as tax_decl_type,
info.PF01ZQ01 as tax_decl_cont,
info.PF01ZR01 as tax_decl_dt,
t1.SID as SID,
t1.STATISTICS_DT as STATISTICS_DT
from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PF01Z.PF01ZH     as data
from POTPF01
)t1,unnest(t1.data) as info(PF01ZD01,PF01ZQ01,PF01ZR01)







select
t1.report_id  as  report_id,
info.PF02ZD01 as civ_decl_type,
info.PF02ZQ01 as civ_decl_cont,
info.PF02ZR01 as civ_decl_dt,
t1.SID as SID,
t1.STATISTICS_DT as STATISTICS_DT
from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PF02Z.PF02ZH     as data
from PCJPF02
)t1,unnest(t1.data) as info(PF02ZD01,PF02ZQ01,PF02ZR01)




select
t1.report_id  as  report_id,
info.PF03ZD01 as enf_decl_type,
info.PF03ZQ01 as enf_decl_cont,
info.PF03ZR01 as enf_decl_dt,
t1.SID as SID,
t1.STATISTICS_DT as STATISTICS_DT
from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PF03Z.PF03ZH        as data
from PCEPF03
)t1,unnest(t1.data) as info(PF03ZD01,PF03ZQ01,PF03ZR01)





select
t1.report_id  as  report_id,
info.PF04ZD01 as punish_decl_type,
info.PF04ZQ01 as punish_decl_cont,
info.PF04ZR01 as punish_decl_dt,
t1.SID as SID,
t1.STATISTICS_DT as STATISTICS_DT
from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PF04Z.PF04ZH     as data
from PAPPF04
)t1,unnest(t1.data) as info(PF04ZD01,PF04ZQ01,PF04ZR01)









select
t1.report_id  as  report_id,
info.PF05ZD01 as housing_decl_type,
info.PF05ZQ01 as housing_decl_cont,
info.PF05ZR01 as housing_decl_dt,
t1.SID as SID,
t1.STATISTICS_DT as STATISTICS_DT
from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PF05Z.PF05ZH     as data
from PHFPF05
)t1,unnest(t1.data) as info(PF05ZD01,PF05ZQ01,PF05ZR01)








select
t1.report_id  as  report_id,
info.PF06ZD01 as allowance_decl_type,
info.PF06ZQ01 as allowance_decl_cont,
info.PF06ZR01 as allowance_decl_dt,
t1.SID as SID,
t1.STATISTICS_DT as STATISTICS_DT
from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PF06Z.PF06ZH     as data
from PBSPF06
)t1,unnest(t1.data) as info(PF06ZD01,PF06ZQ01,PF06ZR01)












-- 模板
select
t1.report_id  as  report_id,
info.PF07ZD01 as qual_decl_type,
info.PF07ZQ01 as qual_decl_cont,
info.PF07ZR01 as qual_decl_dt,
t1.SID as SID,
t1.STATISTICS_DT as STATISTICS_DT
from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PF07Z.PF07ZH     as data
from PPQPF07
)t1,unnest(t1.data) as info(PF07ZD01,PF07ZQ01,PF07ZR01)




select
t1.report_id  as  report_id,
info.PF08ZD01 as rew_decl_type,
info.PF08ZQ01 as rew_decl_cont,
info.PF08ZR01 as rew_decl_dt,
t1.SID as SID,
t1.STATISTICS_DT as STATISTICS_DT
from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PF08Z.PF08ZH     as data
from PAHPF08
)t1,unnest(t1.data) as info(PF08ZD01,PF08ZQ01,PF08ZR01)





select
t1.report_id                            as report_id,
info.PG010D01                           as oth_type_cd,
info.PG010D02                           as oth_sign_cd,
cast(info.PG010S01 as bigint)           as oth_decl_num,
t1.STATISTICS_DT                        as STATISTICS_DT
from (
select
PRH.PA01.PA01A.PA01AI01  				as report_id,
POS.PG01         as data,
'2020-09-27'                            as STATISTICS_DT
from ods_table
)t1,unnest(t1.data) as info(PG010D01,PG010D02,PG010S01,PG010H)




select
t2.report_id                            as report_id,
info2.PG010D03                          as oth_decl_type_cd,
info2.PG010Q01                          as oth_decl_cont,
info2.PG010R01                          as oth_decl_dt,
t2.SID                                  as SID,
t2.STATISTICS_DT                        as STATISTICS_DT
from(
select
t1.report_id                            as report_id,
info.PG010H                             as data2,
t1.SID                                  as SID,
t1.STATISTICS_DT                        as STATISTICS_DT
from (
select
PRH.PA01.PA01A.PA01AI01  				as report_id,
POS.PG01         as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
from ods_table
)t1,unnest(t1.data) as info(PG010D01,PG010D02,PG010S01,PG010H)
)t2,unnest(t2.data2) as info2(PG010D03,PG010Q01,PG010R01)





