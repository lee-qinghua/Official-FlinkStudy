select
PRH.PA01.PA01A.PA01AI01  				as report_id,
PRH.PA01.PA01C.PA01CH         as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
)t1,unnest(t1.data) as info(PA01CD01,PA01CI01)

select
PRH.PA01.PA01A.PA01AI01  				as report_id,
POQ.PH01         as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
)t1,unnest(t1.data) as info(PH010R01,PH010D01,PH010Q02,PH010Q03)

select
PRH.PA01.PA01A.PA01AI01  				as report_id,
POS.PG01         as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
)t1,unnest(t1.data) as info(PG010D01,PG010D02)

select
PRH.PA01.PA01A.PA01AI01  				as report_id,
PPO.PC040H         as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
)t1,unnest(t1.data) as info(PC040D01,PC040S02,PC040J01)

select
PRH.PA01.PA01A.PA01AI01  				as report_id,
PNO.PC030H         as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
)t1,unnest(t1.data) as info(PC030D01,PC030S02,PC030J01)

select
PRH.PA01.PA01A.PA01AI01  				as report_id,
PCO.PC02.PC02K.PC02KH         as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
)t1,unnest(t1.data) as info(PC02KD01,PC02KD02,PC02KS02,PC02KJ01,PC02KJ02)

select
PRH.PA01.PA01A.PA01AI01  				as report_id,
PCO.PC02.PC02D.PC02DH         as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
)t1,unnest(t1.data) as info(PC02DD01,PC02DS02,PC02DS03,PC02DJ01,PC02DS04)

select
PRH.PA01.PA01A.PA01AI01  				as report_id,
PCO.PC02.PC02B.PC02BH         as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
)t1,unnest(t1.data) as info(PC02BD01,PC02BS03,PC02BJ02)

select
PRH.PA01.PA01A.PA01AI01  				as report_id,
PCO.PC02.PC02A.PC02AH         as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
)t1,unnest(t1.data) as info(PC02AD01,PC02AD02,PC02AS03,PC02AR01)

select
PRH.PA01.PA01A.PA01AI01  				as report_id,
POM.PB04         as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
)t1,unnest(t1.data) as info(PB040D01,PB040Q01,PB040D02,PB040D03,PB040Q02,PB040Q03,PB040D04,PB040D05,PB040D06,PB040R01,PB040R02)

select
PRH.PA01.PA01A.PA01AI01  				as report_id,
PRM.PB03         as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
)t1,unnest(t1.data) as info(PB030D01,PB030Q01,PB030Q02,PB030R01)

select
PRH.PA01.PA01A.PA01AI01  				as report_id,
PIM.PB01.PB01B.PB01BH         as data,
PRH.PA01.PA01A.PA01AI01  				as SID,
'2020-09-27'                            as STATISTICS_DT
)t1,unnest(t1.data) as info(PB01BQ01,PB01BR01)

