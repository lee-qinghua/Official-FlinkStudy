
CREATE TABLE t_1144_1 (
   -- 最主要的是这行,定义类型为ROW
   `data` ROW(deviceId string,`data` ROW(mac_value string,ad_name string,voice string,dataTimeStamp string)),
   -- 通过xx.xx取数据,映射成自己想要的表
    deviceId as cast(`data`.deviceId as varchar) ,
    mac_value as cast( `data`.`data`.mac_value as integer),
    ad_name as cast( `data`.`data`.ad_name as integer),
    voice as cast( `data`.`data`.voice as integer),
    eventTime  as cast( `data`.`data`.`dataTimeStamp` as varchar),
     -- 时间窗口以及水位线
    windowEventTime AS TO_TIMESTAMP(FROM_UNIXTIME((cast(cast( `data`.`data`.`dataTimeStamp` as varchar) as bigint)+43200000)/1000)),
    WATERMARK FOR windowEventTime AS windowEventTime - INTERVAL '2' SECOND
) WITH (
     xxxx
)
 后续的sql,就可以基于这个表来进行操作了.


create table test4(
  name string,
  age bigint,
  hobies ROW(movie string,food string),
  company ARRAY<ROW(name string,food string)>
)with(
'connector' = 'filesystem',
'path' = 'file:///D:\peoject\Official-FlinkStudy\flink-1.11\src\main\java\com\otis\work\date20200925解析json\d.json',
'format' = 'json'
)


{"name":"liqinghua","age":18,"hobies":{"movie":"m1","food":"firedraon"},"company":[{"mname":"gongsi1","mtime":"1"},{"mname":"gongsi2","mtime":"2"}]}