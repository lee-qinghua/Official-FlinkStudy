drop table source_table_1;
CREATE TABLE source_table_1 (
      event_id STRING ,
      transtype string,
      amount BIGINT,
      cd BIGINT,
      txn_dt string,
      ts  BIGINT,
      et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')),
      WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )
    WITH (
      'connector' = 'kafka',
      'topic' = 'qinghua001_source_table_2',
      'properties.group.id'='dev_flink',
      'properties.zookeeper.connect'='10.1.30.6:2181',
      'properties.bootstrap.servers' = '10.1.30.8:9092',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset'
      );

drop table sinke_table_1;
CREATE TABLE sinke_table_1 (
      aaa STRING ,
      amount BIGINT
    )
    WITH (
      'connector' = 'kafka',
      'topic' = 'qinghua001_sink_table_2',
      'properties.group.id'='dev_flink',
      'properties.zookeeper.connect'='10.1.30.6:2181',
      'properties.bootstrap.servers' = '10.1.30.8:9092',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset'
      );

insert into sinke_table_1
select
transtype,
sum(amount) over (partition by transtype order by et range between interval '5' second preceding and current row)
from source_table_1;