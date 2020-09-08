-- source表
CREATE TABLE qinghua_source_table_1 (
      event_id string ,
      card_num string,
      transtype string,
      amount BIGINT,
      txn_cd BIGINT,
      txn_dt string, --日期   DAYOFWEEK(DATE '1994-09-27') returns 3  sunday=1
      txn_tm BIGINT, --时间   HOUR(TIMESTAMP '1994-09-27 13:14:15') returns 13
      use_method_cd string, -- 现在没有对应的关系，001：线上第三方 002：线上云闪付 003：线上银行自由渠道 004：线下POS刷卡
      ts  BIGINT,
      et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')),
      WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )
    WITH (
      'connector' = 'kafka',
      'topic' = 'qinghua002_source_table',
      'properties.group.id'='dev_flink',
      'properties.zookeeper.connect'='10.1.30.6:2181',
      'properties.bootstrap.servers' = '10.1.30.8:9092',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset'
      );
-- sink表
create table qinghua_sink_table_1(
    card_num STRING,
    ten_min_count BIGINT,
    ten_min_avg BIGINT,
    thirty_min_count BIGINT,
    thirty_min_avg BIGINT,
    one_hour_count BIGINT,
    one_hour_avg BIGINT,
    one_day_count BIGINT,
    one_day_avg BIGINT,
    two_1 string,
    two_2 string,
    two_3 string,
    two_4 string,
    two_5 string,
    five_1 string,
    five_2 string,
    five_3 string,
    five_4 string,
    five_5 string,
    ten_1 string,
    ten_2 string,
    ten_3 string,
    ten_4 string,
    ten_5 string,
    is_earlymorning string,
    is_sundaymorning string,
    trans_type string,
    is_overconsume string
)
WITH (
      'connector' = 'kafka',
      'topic' = 'otis_test_sink_table',
      'properties.group.id'='dev_flink',
      'properties.zookeeper.connect'='10.1.30.6:2181',
      'properties.bootstrap.servers' = '10.1.30.8:9092',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset'
      );

--注册所需的udf
create function transamount_udaf as 'com.otis.work.date20200902udf.MyFunction';

-- todo 创建四个kafka table
CREATE TABLE kafka_table_1 (
       card_num STRING,
       ts BIGINT,
       mcount BIGINT ,
       avgamount BIGINT,
       two_1 STRING,
       two_2 STRING,
       two_3 STRING,
       two_4 STRING,
       two_5 STRING,
       five_1 STRING,
       five_2 STRING,
       five_3 STRING,
       five_4 STRING,
       five_5 STRING,
       ten_1 STRING,
       ten_2 STRING,
       ten_3 STRING,
       ten_4 STRING,
       ten_5 STRING,
       is_earlymorning STRING,
       is_sundaymorning STRING,
       trans_type STRING,
       is_overconsume STRING,
       et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')),
       WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )
    WITH (
      'connector' = 'kafka',
      'topic' = 'kafka_table_1',
      'properties.group.id'='dev_flink',
      'properties.zookeeper.connect'='10.1.30.6:2181',
      'properties.bootstrap.servers' = '10.1.30.8:9092',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset'
      );

-- 10分钟的相关数据
insert into kafka_table_1
select
    card_num,
    ts,
    count(txn_cd) over w                               as  mcount,
    avg(amount) over w                               as  avgamount,
    SPLIT_INDEX(transamount_udaf(cast(amount as int),-1)over w, ',', 0) as  two_1,
    SPLIT_INDEX(transamount_udaf(cast(amount as int),-1)over w, ',', 1) as  two_2,
    SPLIT_INDEX(transamount_udaf(cast(amount as int),-1)over w, ',', 2) as  two_3,
    SPLIT_INDEX(transamount_udaf(cast(amount as int),-1)over w, ',', 3) as  two_4,
    SPLIT_INDEX(transamount_udaf(cast(amount as int),-1)over w, ',', 4) as  two_5,
    SPLIT_INDEX(transamount_udaf(cast(amount as int),-1)over w, ',', 5) as  five_1,
    SPLIT_INDEX(transamount_udaf(cast(amount as int),-1)over w, ',', 6) as  five_2,
    SPLIT_INDEX(transamount_udaf(cast(amount as int),-1)over w, ',', 7) as  five_3,
    SPLIT_INDEX(transamount_udaf(cast(amount as int),-1)over w, ',', 8) as  five_4,
    SPLIT_INDEX(transamount_udaf(cast(amount as int),-1)over w, ',', 9) as  five_5,
    SPLIT_INDEX(transamount_udaf(cast(amount as int),-1)over w, ',', 10) as  ten_1,
    SPLIT_INDEX(transamount_udaf(cast(amount as int),-1)over w, ',', 11) as  ten_2,
    SPLIT_INDEX(transamount_udaf(cast(amount as int),-1)over w, ',', 12) as  ten_3,
    SPLIT_INDEX(transamount_udaf(cast(amount as int),-1)over w, ',', 13) as  ten_4,
    SPLIT_INDEX(transamount_udaf(cast(amount as int),-1)over w, ',', 14) as  ten_5,
    if(txn_tm>1 and txn_tm<6,'是','否')               as  is_earlymorning,
    if(DAYOFWEEK(DATE '2020-02-03')=1 and txn_tm>5 and txn_tm<8,'是','否') as is_sundaymorning,-- todo 这里的时间字段先写死，不知道什么格式
    case when use_method_cd='001' then '线上第三方'
         when use_method_cd='002' then '线上云闪付'
         when use_method_cd='003' then '线上银行自由渠道'
         when use_method_cd='004' then '线下POS刷卡' end as trans_type,
    if(amount>20000,'是','否')                           as is_overconsume -- todo 授信额度80%还没有加上，授信额度字段不知道
from qinghua_source_table_1
window w as (partition by card_num order by et range between interval '10' minute preceding and current row);

-- 30分钟的相关数据
CREATE TABLE kafka_table_2 (
       card_num STRING,
       ts BIGINT,
       mcount BIGINT ,
       avgamount BIGINT,
       et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')),
       WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )
    WITH (
      'connector' = 'kafka',
      'topic' = 'kafka_table_2',
      'properties.group.id'='dev_flink',
      'properties.zookeeper.connect'='10.1.30.6:2181',
      'properties.bootstrap.servers' = '10.1.30.8:9092',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset'
      );

insert into kafka_table_2
select
    card_num,
    ts,
    count(txn_cd) over w as mcount,
    avg(amount) over w as avgamount
from qinghua_source_table_1
window w as (partition by card_num order by et range between interval '30' minute preceding and current row);
-- 1小时的相关数据
CREATE TABLE kafka_table_3 (
       card_num STRING,
       ts BIGINT,
       mcount BIGINT ,
       avgamount BIGINT,
       et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')),
       WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )
    WITH (
      'connector' = 'kafka',
      'topic' = 'kafka_table_3',
      'properties.group.id'='dev_flink',
      'properties.zookeeper.connect'='10.1.30.6:2181',
      'properties.bootstrap.servers' = '10.1.30.8:9092',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset'
      );

insert into kafka_table_3
select
    card_num,
    ts,
    count(txn_cd) over w as mcount,
    avg(amount) over w as avgamount
from qinghua_source_table_1
window w as (partition by card_num order by et range between interval '1' hour preceding and current row);
-- 1天的相关数据

CREATE TABLE kafka_table_4 (
       card_num STRING,
       ts BIGINT,
       mcount BIGINT ,
       avgamount BIGINT,
       et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')),
       WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )
    WITH (
      'connector' = 'kafka',
      'topic' = 'kafka_table_4',
      'properties.group.id'='dev_flink',
      'properties.zookeeper.connect'='10.1.30.6:2181',
      'properties.bootstrap.servers' = '10.1.30.8:9092',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset'
      );

insert into kafka_table_4
select
    card_num,
    ts,
    count(txn_cd) over w as mcount,
    avg(amount) over w as avgamount
from qinghua_source_table_1
window w as (partition by card_num order by et range between interval '1' day preceding and current row);


-- todo 四个view进行join总结到一张表中
-- 10分钟和30分钟join
CREATE TABLE kafka_table_5 (
       card_num STRING,
       ts BIGINT,
       ten_min_count BIGINT ,
       ten_min_avg BIGINT,
       thirty_min_count BIGINT,
       thirty_min_avg BIGINT,
       two_1 STRING,
       two_2 STRING,
       two_3 STRING,
       two_4 STRING,
       two_5 STRING,
       five_1 STRING,
       five_2 STRING,
       five_3 STRING,
       five_4 STRING,
       five_5 STRING,
       ten_1 STRING,
       ten_2 STRING,
       ten_3 STRING,
       ten_4 STRING,
       ten_5 STRING,
       is_earlymorning STRING,
       is_sundaymorning STRING,
       trans_type STRING,
       is_overconsume STRING,
       et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')),
       WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )
    WITH (
      'connector' = 'kafka',
      'topic' = 'kafka_table_5',
      'properties.group.id'='dev_flink',
      'properties.zookeeper.connect'='10.1.30.6:2181',
      'properties.bootstrap.servers' = '10.1.30.8:9092',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset'
      );
insert into kafka_table_5
select
    a.card_num as card_num,
    a.ts as ts,
    a.mcount as ten_min_count,
    a.avgamount as ten_min_avg,
    b.mcount as thirty_min_count,
    b.avgamount as thirty_min_avg,
    a.two_1     as          two_1,
    a.two_2     as          two_2,
    a.two_3     as          two_3,
    a.two_4     as          two_4,
    a.two_5     as          two_5,
    a.five_1    as          five_1,
    a.five_2    as          five_2,
    a.five_3    as          five_3,
    a.five_4    as          five_4,
    a.five_5    as          five_5,
    a.ten_1     as          ten_1,
    a.ten_2     as          ten_2,
    a.ten_3     as          ten_3,
    a.ten_4     as          ten_4,
    a.ten_5     as          ten_5,
    a.is_earlymorning as    is_earlymorning,
    a.is_sundaymorning as   is_sundaymorning,
    a.trans_type as         trans_type,
    a.is_overconsume as     is_overconsume
from kafka_table_1 a,kafka_table_2 b
where a.card_num=b.card_num
and a.et BETWEEN b.et - INTERVAL '0' second AND b.et + INTERVAL '2' second;



-- 上次结果和1小时join
CREATE TABLE kafka_table_6 (
       card_num STRING,
       ts BIGINT,
       ten_min_count BIGINT ,
       ten_min_avg BIGINT,
       thirty_min_count BIGINT,
       thirty_min_avg BIGINT,
       thirty_min_count BIGINT,
       thirty_min_avg BIGINT,
       one_hour_count BIGINT,
       one_hour_avg BIGINT,
       two_1 STRING,
       two_2 STRING,
       two_3 STRING,
       two_4 STRING,
       two_5 STRING,
       five_1 STRING,
       five_2 STRING,
       five_3 STRING,
       five_4 STRING,
       five_5 STRING,
       ten_1 STRING,
       ten_2 STRING,
       ten_3 STRING,
       ten_4 STRING,
       ten_5 STRING,
       is_earlymorning STRING,
       is_sundaymorning STRING,
       trans_type STRING,
       is_overconsume STRING,
       et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')),
       WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )
    WITH (
      'connector' = 'kafka',
      'topic' = 'kafka_table_6',
      'properties.group.id'='dev_flink',
      'properties.zookeeper.connect'='10.1.30.6:2181',
      'properties.bootstrap.servers' = '10.1.30.8:9092',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset'
      );

insert into kafka_table_6
select
    c.card_num as card_num,
    c.ts as ts,
    c.ten_min_count as ten_min_count,
    c.ten_min_avg as ten_min_avg,
    c.thirty_min_count as thirty_min_count,
    c.thirty_min_avg as thirty_min_avg,
    d.mcount as one_hour_count,
    d.avgamount as one_hour_avg,
    c.two_1     as          two_1,
    c.two_2     as          two_2,
    c.two_3     as          two_3,
    c.two_4     as          two_4,
    c.two_5     as          two_5,
    c.five_1    as          five_1,
    c.five_2    as          five_2,
    c.five_3    as          five_3,
    c.five_4    as          five_4,
    c.five_5    as          five_5,
    c.ten_1     as          ten_1,
    c.ten_2     as          ten_2,
    c.ten_3     as          ten_3,
    c.ten_4     as          ten_4,
    c.ten_5     as          ten_5,
    c.is_earlymorning as    is_earlymorning,
    c.is_sundaymorning as   is_sundaymorning,
    c.trans_type as         trans_type,
    c.is_overconsume as     is_overconsume
from kafka_table_5 c,kafka_table_3 d
where c.card_num=d.card_num
and c.et BETWEEN d.et - INTERVAL '2' second AND d.et + INTERVAL '2' second;

-- mid_table_2 和1天join
-- create view mid_table_3 as
insert into qinghua_sink_table_1
select
    e.card_num as card_num,
    e.ten_min_count as ten_min_count,
    e.ten_min_avg as ten_min_avg,
    e.thirty_min_count as thirty_min_count,
    e.thirty_min_avg as thirty_min_avg,
    e.one_hour_count as one_hour_count,
    e.one_hour_avg as one_hour_avg,
    f.mcount as one_day_count,
    f.avgamount as one_day_avg,
    e.two_1     as          two_1,
    e.two_2     as          two_2,
    e.two_3     as          two_3,
    e.two_4     as          two_4,
    e.two_5     as          two_5,
    e.five_1    as          five_1,
    e.five_2    as          five_2,
    e.five_3    as          five_3,
    e.five_4    as          five_4,
    e.five_5    as          five_5,
    e.ten_1     as          ten_1,
    e.ten_2     as          ten_2,
    e.ten_3     as          ten_3,
    e.ten_4     as          ten_4,
    e.ten_5     as          ten_5,
    e.is_earlymorning as    is_earlymorning,
    e.is_sundaymorning as   is_sundaymorning,
    e.trans_type as         trans_type,
    e.is_overconsume as     is_overconsume
from kafka_table_6 as e,kafka_table_4 as f
where e.card_num=f.card_num
and e.et between f.et - interval '2' second and f.et + interval '2' second;