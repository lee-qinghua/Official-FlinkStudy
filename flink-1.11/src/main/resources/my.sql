-- source表
CREATE TABLE source_table_1 (
      event_id STRING COMMIT '事件编号',
      cd_org_cd STRING COMMIT '信用卡ORG代码',
      acct_num STRING COMMIT '账号',
      acct_modifier_num STRING COMMIT '账号修饰符',
      cust_id STRING COMMIT '客户号',
      card_num STRING COMMIT '卡号',
      card_seq_num STRING COMMIT '卡序号',
      logo_cd STRING COMMIT 'LOGO代码',
      subbranch_id STRING COMMIT '账户所属支行',
      txn_ref_num STRING COMMIT '金融交易参考号',
      txn_type_cd STRING COMMIT '金融交易类型代码',
      txn_dt STRING COMMIT '交易日期',
      batch_dt STRING COMMIT '批量日期',
      posting_dt STRING COMMIT '入账日期',
      cd_plan_num STRING COMMIT '信用计划号',
      txn_cd STRING COMMIT '交易代码',
      logic_module_cd STRING COMMIT '逻辑模块代码',
      txn_amt STRING COMMIT '交易金额',
      rmb_txn_amt STRING COMMIT '交易金额折人民币',
      auth_num    STRING    COMMIT '授权号',
      lty_pgm_num    STRING    COMMIT '积分计划编号',
      txn_pts_nbr    DECIMAL(18,2)    COMMIT '事件积分',
      external_txn_id    STRING    COMMIT '外部交易ID',
      mcc_cd    STRING    COMMIT 'MCC代码',
      equip_card_num    STRING    COMMIT '设备卡号',
      equip_card_type_cd    STRING    COMMIT '设备卡类型代码',
      merchant_country_cd    STRING    COMMIT '商户国家代码',
      merchant_id    STRING    COMMIT '商户编号',
      txn_desc    STRING    COMMIT '交易描述',
      txn_terminal_num    STRING    COMMIT '交易终端编号',
      gl_src_cd    STRING    COMMIT '总账来源代码',
      txn_src_cd    STRING    COMMIT '交易来源代码',
      txn_src_type_cd    STRING    COMMIT '交易来源分类代码',
      currency_conv_type_cd    STRING    COMMIT '货币转换类型代码',
      ec_txn_type_cd    STRING    COMMIT '电子商务交易类型代码',
      curr_conv_charge_mode_cd    STRING    COMMIT '货币转换收费方式代码',
      use_method_cd    STRING    COMMIT '使用方式代码',
      deal_bank_org_id    STRING    COMMIT '受理行机构编号',
      union_pay_sys_trace_num    STRING    COMMIT '银联系统跟踪号',
      spec_fee_type    STRING    COMMIT '特殊计费类型',
      spec_fee_level    STRING    COMMIT '特殊计费档次',
      txn_tm    STRING    COMMIT '交易时间',
      pbc_remittance_biz_id    STRING    COMMIT '人行大小额汇款业务编号',
      extend_merc_name    STRING    COMMIT '扩展商户名称',
      level2_merc_name    STRING    COMMIT '二级商户名称',
      biz_product_type_cd    STRING    COMMIT '业务产品类型代码',
      payer_name    STRING    COMMIT '付款方姓名',
      pay_acct_num    STRING    COMMIT '付款账号',
      payment_remark    STRING    COMMIT '代付附言',
      pbcc_nc_txn_seq_num    STRING    COMMIT '人行网联交易流水号',
      nc_instalment_fee_type_cd    STRING    COMMIT '网联分期手续费收取类型代码',
      nc_instalment_prod_cd    STRING COMMIT '网联分期分期产品代码',
      acqr_bank_id    STRING    COMMIT '收单行行号',
      pay_acct_type_cd    STRING    COMMIT '付款账号类型代码',
      app_service_id    STRING    COMMIT '应用服务方ID',
      org_name    STRING    COMMIT '机构名称',
      biz_desc    STRING    COMMIT '业务描述',
      new_borrow_flage    INT    COMMIT '借贷标记',
      new_account_chinessname STRING  COMMIT '账户中文名',
      match_auth_txn_ind    STRING    COMMIT '匹配到授权交易标志',
      data_dt    STRING    COMMIT '数据日期'
      et AS TO_TIMESTAMP(FROM_UNIXTIME(cast(data_dt as bigint)/1000)),
      WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )
    WITH (
      'connector' = 'kafka',
      'topic' = 'source_table_1',
      'properties.group.id'='dev_flink',
      'properties.zookeeper.connect'='10.1.30.6:2181',
      'properties.bootstrap.servers' = '10.1.30.8:9092',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset'
      );
-- 结果表
create table sink_table_1(
		xiaofei_01 INTEGER COMMIT '最近1天&Transtype.交易笔数',
		xiaofei_02 decimal COMMIT '最近1天&Transtype.交易金额',
		xiaofei_03 decimal COMMIT '最近1天&Transtype.最大交易金额',
		xiaofei_04 INTEGER COMMIT '最近1天&Transtype.发生交易天数',
		xiaofei_05 INTEGER COMMIT '最近1天的日均&Transtype.交易笔数',
		xiaofei_06 decimal COMMIT '最近1天的日均&Transtype.交易金额',
		xiaofei_07 FLOAT COMMIT '最近1天的&Transtype.交易金额占授信额度比例',
		xiaofei_08 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易笔数的比例',
		xiaofei_09 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易金额的比例',
		xiaofei_11 INTEGER COMMIT '最近3天&Transtype.交易笔数',
		xiaofei_12 DECIMAL(18,2) COMMIT '最近3天&Transtype.交易金额',
		xiaofei_13 DECIMAL(18,2) COMMIT '最近3天&Transtype.最大交易金额',
		xiaofei_14 INTEGER COMMIT '最近3天&Transtype.发生交易天数',
		xiaofei_15 INTEGER COMMIT '最近3天的日均&Transtype.交易笔数',
		xiaofei_16 DECIMAL(18,2) COMMIT '最近3天的日均&Transtype.交易金额',
		xiaofei_17 FLOAT COMMIT '最近3天的&Transtype.交易金额占授信额度比例',
		xiaofei_18 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易笔数的比例',
		xiaofei_19 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易金额的比例',
		xiaofei_20 STRING COMMIT '是否最近3天连续&Transtype.',
		xiaofei_21 STRING COMMIT '是否最近3天连续&Transtype.金额大于5万',
		xiaofei_22 STRING COMMIT '是否最近3天连续&Transtype.金额大于10万',
		xiaofei_23 STRING COMMIT '是否最近3天连续&Transtype.金额大于25万',
		xiaofei_24 STRING COMMIT '是否最近3天连续&Transtype.金额大于50万',
		bangong_01 INTEGER COMMIT '最近1天&Transtype.交易笔数',
		bangong_02 decimal COMMIT '最近1天&Transtype.交易金额',
		bangong_03 decimal COMMIT '最近1天&Transtype.最大交易金额',
		bangong_04 INTEGER COMMIT '最近1天&Transtype.发生交易天数',
		bangong_05 INTEGER COMMIT '最近1天的日均&Transtype.交易笔数',
		bangong_06 decimal COMMIT '最近1天的日均&Transtype.交易金额',
		bangong_07 FLOAT COMMIT '最近1天的&Transtype.交易金额占授信额度比例',
		bangong_08 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易笔数的比例',
		bangong_09 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易金额的比例',
		bangong_11 INTEGER COMMIT '最近3天&Transtype.交易笔数',
		bangong_12 DECIMAL(18,2) COMMIT '最近3天&Transtype.交易金额',
		bangong_13 DECIMAL(18,2) COMMIT '最近3天&Transtype.最大交易金额',
		bangong_14 INTEGER COMMIT '最近3天&Transtype.发生交易天数',
		bangong_15 INTEGER COMMIT '最近3天的日均&Transtype.交易笔数',
		bangong_16 DECIMAL(18,2) COMMIT '最近3天的日均&Transtype.交易金额',
		bangong_17 FLOAT COMMIT '最近3天的&Transtype.交易金额占授信额度比例',
		bangong_18 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易笔数的比例',
		bangong_19 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易金额的比例',
		bangong_20 STRING COMMIT '是否最近3天连续&Transtype.',
		bangong_21 STRING COMMIT '是否最近3天连续&Transtype.金额大于5万',
		bangong_22 STRING COMMIT '是否最近3天连续&Transtype.金额大于10万',
		bangong_23 STRING COMMIT '是否最近3天连续&Transtype.金额大于25万',
		bangong_24 STRING COMMIT '是否最近3天连续&Transtype.金额大于50万',
		xinzi_01 INTEGER COMMIT '最近1天&Transtype.交易笔数',
		xinzi_02 decimal COMMIT '最近1天&Transtype.交易金额',
		xinzi_03 decimal COMMIT '最近1天&Transtype.最大交易金额',
		xinzi_04 INTEGER COMMIT '最近1天&Transtype.发生交易天数',
		xinzi_05 INTEGER COMMIT '最近1天的日均&Transtype.交易笔数',
		xinzi_06 decimal COMMIT '最近1天的日均&Transtype.交易金额',
		xinzi_07 FLOAT COMMIT '最近1天的&Transtype.交易金额占授信额度比例',
		xinzi_08 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易笔数的比例',
		xinzi_09 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易金额的比例',
		xinzi_11 INTEGER COMMIT '最近3天&Transtype.交易笔数',
		xinzi_12 DECIMAL(18,2) COMMIT '最近3天&Transtype.交易金额',
		xinzi_13 DECIMAL(18,2) COMMIT '最近3天&Transtype.最大交易金额',
		xinzi_14 INTEGER COMMIT '最近3天&Transtype.发生交易天数',
		xinzi_15 INTEGER COMMIT '最近3天的日均&Transtype.交易笔数',
		xinzi_16 DECIMAL(18,2) COMMIT '最近3天的日均&Transtype.交易金额',
		xinzi_17 FLOAT COMMIT '最近3天的&Transtype.交易金额占授信额度比例',
		xinzi_18 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易笔数的比例',
		xinzi_19 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易金额的比例',
		xinzi_20 STRING COMMIT '是否最近3天连续&Transtype.',
		xinzi_21 STRING COMMIT '是否最近3天连续&Transtype.金额大于5万',
		xinzi_22 STRING COMMIT '是否最近3天连续&Transtype.金额大于10万',
		xinzi_23 STRING COMMIT '是否最近3天连续&Transtype.金额大于25万',
		xinzi_24 STRING COMMIT '是否最近3天连续&Transtype.金额大于50万',
		baoxian_01 INTEGER COMMIT '最近1天&Transtype.交易笔数',
		baoxian_02 decimal COMMIT '最近1天&Transtype.交易金额',
		baoxian_03 decimal COMMIT '最近1天&Transtype.最大交易金额',
		baoxian_04 INTEGER COMMIT '最近1天&Transtype.发生交易天数',
		baoxian_05 INTEGER COMMIT '最近1天的日均&Transtype.交易笔数',
		baoxian_06 decimal COMMIT '最近1天的日均&Transtype.交易金额',
		baoxian_07 FLOAT COMMIT '最近1天的&Transtype.交易金额占授信额度比例',
		baoxian_08 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易笔数的比例',
		baoxian_09 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易金额的比例',
		baoxian_11 INTEGER COMMIT '最近3天&Transtype.交易笔数',
		baoxian_12 DECIMAL(18,2) COMMIT '最近3天&Transtype.交易金额',
		baoxian_13 DECIMAL(18,2) COMMIT '最近3天&Transtype.最大交易金额',
		baoxian_14 INTEGER COMMIT '最近3天&Transtype.发生交易天数',
		baoxian_15 INTEGER COMMIT '最近3天的日均&Transtype.交易笔数',
		baoxian_16 DECIMAL(18,2) COMMIT '最近3天的日均&Transtype.交易金额',
		baoxian_17 FLOAT COMMIT '最近3天的&Transtype.交易金额占授信额度比例',
		baoxian_18 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易笔数的比例',
		baoxian_19 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易金额的比例',
		baoxian_20 STRING COMMIT '是否最近3天连续&Transtype.',
		baoxian_21 STRING COMMIT '是否最近3天连续&Transtype.金额大于5万',
		baoxian_22 STRING COMMIT '是否最近3天连续&Transtype.金额大于10万',
		baoxian_23 STRING COMMIT '是否最近3天连续&Transtype.金额大于25万',
		baoxian_24 STRING COMMIT '是否最近3天连续&Transtype.金额大于50万',
		touzi_01 INTEGER COMMIT '最近1天&Transtype.交易笔数',
		touzi_02 decimal COMMIT '最近1天&Transtype.交易金额',
		touzi_03 decimal COMMIT '最近1天&Transtype.最大交易金额',
		touzi_04 INTEGER COMMIT '最近1天&Transtype.发生交易天数',
		touzi_05 INTEGER COMMIT '最近1天的日均&Transtype.交易笔数',
		touzi_06 decimal COMMIT '最近1天的日均&Transtype.交易金额',
		touzi_07 FLOAT COMMIT '最近1天的&Transtype.交易金额占授信额度比例',
		touzi_08 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易笔数的比例',
		touzi_09 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易金额的比例',
		touzi_11 INTEGER COMMIT '最近3天&Transtype.交易笔数',
		touzi_12 DECIMAL(18,2) COMMIT '最近3天&Transtype.交易金额',
		touzi_13 DECIMAL(18,2) COMMIT '最近3天&Transtype.最大交易金额',
		touzi_14 INTEGER COMMIT '最近3天&Transtype.发生交易天数',
		touzi_15 INTEGER COMMIT '最近3天的日均&Transtype.交易笔数',
		touzi_16 DECIMAL(18,2) COMMIT '最近3天的日均&Transtype.交易金额',
		touzi_17 FLOAT COMMIT '最近3天的&Transtype.交易金额占授信额度比例',
		touzi_18 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易笔数的比例',
		touzi_19 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易金额的比例',
		touzi_20 STRING COMMIT '是否最近3天连续&Transtype.',
		touzi_21 STRING COMMIT '是否最近3天连续&Transtype.金额大于5万',
		touzi_22 STRING COMMIT '是否最近3天连续&Transtype.金额大于10万',
		touzi_23 STRING COMMIT '是否最近3天连续&Transtype.金额大于25万',
		touzi_24 STRING COMMIT '是否最近3天连续&Transtype.金额大于50万',
		rongzi_01 INTEGER COMMIT '最近1天&Transtype.交易笔数',
		rongzi_02 decimal COMMIT '最近1天&Transtype.交易金额',
		rongzi_03 decimal COMMIT '最近1天&Transtype.最大交易金额',
		rongzi_04 INTEGER COMMIT '最近1天&Transtype.发生交易天数',
		rongzi_05 INTEGER COMMIT '最近1天的日均&Transtype.交易笔数',
		rongzi_06 decimal COMMIT '最近1天的日均&Transtype.交易金额',
		rongzi_07 FLOAT COMMIT '最近1天的&Transtype.交易金额占授信额度比例',
		rongzi_08 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易笔数的比例',
		rongzi_09 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易金额的比例',
		rongzi_11 INTEGER COMMIT '最近3天&Transtype.交易笔数',
		rongzi_12 DECIMAL(18,2) COMMIT '最近3天&Transtype.交易金额',
		rongzi_13 DECIMAL(18,2) COMMIT '最近3天&Transtype.最大交易金额',
		rongzi_14 INTEGER COMMIT '最近3天&Transtype.发生交易天数',
		rongzi_15 INTEGER COMMIT '最近3天的日均&Transtype.交易笔数',
		rongzi_16 DECIMAL(18,2) COMMIT '最近3天的日均&Transtype.交易金额',
		rongzi_17 FLOAT COMMIT '最近3天的&Transtype.交易金额占授信额度比例',
		rongzi_18 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易笔数的比例',
		rongzi_19 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易金额的比例',
		rongzi_20 STRING COMMIT '是否最近3天连续&Transtype.',
		rongzi_21 STRING COMMIT '是否最近3天连续&Transtype.金额大于5万',
		rongzi_22 STRING COMMIT '是否最近3天连续&Transtype.金额大于10万',
		rongzi_23 STRING COMMIT '是否最近3天连续&Transtype.金额大于25万',
		rongzi_24 STRING COMMIT '是否最近3天连续&Transtype.金额大于50万',
		zhuanzhang_01 INTEGER COMMIT '最近1天&Transtype.交易笔数',
		zhuanzhang_02 decimal COMMIT '最近1天&Transtype.交易金额',
		zhuanzhang_03 decimal COMMIT '最近1天&Transtype.最大交易金额',
		zhuanzhang_04 INTEGER COMMIT '最近1天&Transtype.发生交易天数',
		zhuanzhang_05 INTEGER COMMIT '最近1天的日均&Transtype.交易笔数',
		zhuanzhang_06 decimal COMMIT '最近1天的日均&Transtype.交易金额',
		zhuanzhang_07 FLOAT COMMIT '最近1天的&Transtype.交易金额占授信额度比例',
		zhuanzhang_08 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易笔数的比例',
		zhuanzhang_09 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易金额的比例',
		zhuanzhang_11 INTEGER COMMIT '最近3天&Transtype.交易笔数',
		zhuanzhang_12 DECIMAL(18,2) COMMIT '最近3天&Transtype.交易金额',
		zhuanzhang_13 DECIMAL(18,2) COMMIT '最近3天&Transtype.最大交易金额',
		zhuanzhang_14 INTEGER COMMIT '最近3天&Transtype.发生交易天数',
		zhuanzhang_15 INTEGER COMMIT '最近3天的日均&Transtype.交易笔数',
		zhuanzhang_16 DECIMAL(18,2) COMMIT '最近3天的日均&Transtype.交易金额',
		zhuanzhang_17 FLOAT COMMIT '最近3天的&Transtype.交易金额占授信额度比例',
		zhuanzhang_18 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易笔数的比例',
		zhuanzhang_19 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易金额的比例',
		zhuanzhang_20 STRING COMMIT '是否最近3天连续&Transtype.',
		zhuanzhang_21 STRING COMMIT '是否最近3天连续&Transtype.金额大于5万',
		zhuanzhang_22 STRING COMMIT '是否最近3天连续&Transtype.金额大于10万',
		zhuanzhang_23 STRING COMMIT '是否最近3天连续&Transtype.金额大于25万',
		zhuanzhang_24 STRING COMMIT '是否最近3天连续&Transtype.金额大于50万',
		cunqu_01 INTEGER COMMIT '最近1天&Transtype.交易笔数',
		cunqu_02 decimal COMMIT '最近1天&Transtype.交易金额',
		cunqu_03 decimal COMMIT '最近1天&Transtype.最大交易金额',
		cunqu_04 INTEGER COMMIT '最近1天&Transtype.发生交易天数',
		cunqu_05 INTEGER COMMIT '最近1天的日均&Transtype.交易笔数',
		cunqu_06 decimal COMMIT '最近1天的日均&Transtype.交易金额',
		cunqu_07 FLOAT COMMIT '最近1天的&Transtype.交易金额占授信额度比例',
		cunqu_08 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易笔数的比例',
		cunqu_09 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易金额的比例',
		cunqu_11 INTEGER COMMIT '最近3天&Transtype.交易笔数',
		cunqu_12 DECIMAL(18,2) COMMIT '最近3天&Transtype.交易金额',
		cunqu_13 DECIMAL(18,2) COMMIT '最近3天&Transtype.最大交易金额',
		cunqu_14 INTEGER COMMIT '最近3天&Transtype.发生交易天数',
		cunqu_15 INTEGER COMMIT '最近3天的日均&Transtype.交易笔数',
		cunqu_16 DECIMAL(18,2) COMMIT '最近3天的日均&Transtype.交易金额',
		cunqu_17 FLOAT COMMIT '最近3天的&Transtype.交易金额占授信额度比例',
		cunqu_18 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易笔数的比例',
		cunqu_19 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易金额的比例',
		cunqu_20 STRING COMMIT '是否最近3天连续&Transtype.',
		cunqu_21 STRING COMMIT '是否最近3天连续&Transtype.金额大于5万',
		cunqu_22 STRING COMMIT '是否最近3天连续&Transtype.金额大于10万',
		cunqu_23 STRING COMMIT '是否最近3天连续&Transtype.金额大于25万',
		cunqu_24 STRING COMMIT '是否最近3天连续&Transtype.金额大于50万',
		other_01 INTEGER COMMIT '最近1天&Transtype.交易笔数',
		other_02 decimal COMMIT '最近1天&Transtype.交易金额',
		other_03 decimal COMMIT '最近1天&Transtype.最大交易金额',
		other_04 INTEGER COMMIT '最近1天&Transtype.发生交易天数',
		other_05 INTEGER COMMIT '最近1天的日均&Transtype.交易笔数',
		other_06 decimal COMMIT '最近1天的日均&Transtype.交易金额',
		other_07 FLOAT COMMIT '最近1天的&Transtype.交易金额占授信额度比例',
		other_08 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易笔数的比例',
		other_09 FLOAT COMMIT '最近1天与最近1天内的日均&Transtype.交易金额的比例',
		other_11 INTEGER COMMIT '最近3天&Transtype.交易笔数',
		other_12 DECIMAL(18,2) COMMIT '最近3天&Transtype.交易金额',
		other_13 DECIMAL(18,2) COMMIT '最近3天&Transtype.最大交易金额',
		other_14 INTEGER COMMIT '最近3天&Transtype.发生交易天数',
		other_15 INTEGER COMMIT '最近3天的日均&Transtype.交易笔数',
		other_16 DECIMAL(18,2) COMMIT '最近3天的日均&Transtype.交易金额',
		other_17 FLOAT COMMIT '最近3天的&Transtype.交易金额占授信额度比例',
		other_18 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易笔数的比例',
		other_19 FLOAT COMMIT '最近3天与最近3天内的日均&Transtype.交易金额的比例',
		other_20 STRING COMMIT '是否最近3天连续&Transtype.',
		other_21 STRING COMMIT '是否最近3天连续&Transtype.金额大于5万',
		other_22 STRING COMMIT '是否最近3天连续&Transtype.金额大于10万',
		other_23 STRING COMMIT '是否最近3天连续&Transtype.金额大于25万',
		other_24 STRING COMMIT '是否最近3天连续&Transtype.金额大于50万')
		WITH (
      'connector' = 'kafka',
      'topic' = 'qinghua_sink_table',
      'properties.group.id'='dev_flink',
      'properties.zookeeper.connect'='10.1.30.6:2181',
      'properties.bootstrap.servers' = '10.1.30.8:9092',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset'
      );

-- 注册函数
create function my_function as 'com.cebbank.airisk.flink.udfs.PreviousValueAggFunction$LongPreviousValueAggFunction';

create view mid_data_table_1 as
select
      event_id,
      my_function(payer_name,acct_num,currency_conv_type_cd,txn_cd,txn_amt,new_borrow_flage,biz_product_type_cd,txn_desc,new_account_chinessname) as rulecode,
	  case
	  when my_function(payer_name,acct_num,currency_conv_type_cd,txn_cd,txn_amt,new_borrow_flage,biz_product_type_cd,txn_desc,new_account_chinessname) like '1%' then 'xiaofei'
	  when my_function(payer_name,acct_num,currency_conv_type_cd,txn_cd,txn_amt,new_borrow_flage,biz_product_type_cd,txn_desc,new_account_chinessname) like '2%' then 'bangong'
	  when my_function(payer_name,acct_num,currency_conv_type_cd,txn_cd,txn_amt,new_borrow_flage,biz_product_type_cd,txn_desc,new_account_chinessname) like '3%' then 'xinzi'
	  when my_function(payer_name,acct_num,currency_conv_type_cd,txn_cd,txn_amt,new_borrow_flage,biz_product_type_cd,txn_desc,new_account_chinessname) like '4%' then 'baoxian'
	  when my_function(payer_name,acct_num,currency_conv_type_cd,txn_cd,txn_amt,new_borrow_flage,biz_product_type_cd,txn_desc,new_account_chinessname) like '5%' then 'touzi'
	  when my_function(payer_name,acct_num,currency_conv_type_cd,txn_cd,txn_amt,new_borrow_flage,biz_product_type_cd,txn_desc,new_account_chinessname) like '6%' then 'rongzi'
	  when my_function(payer_name,acct_num,currency_conv_type_cd,txn_cd,txn_amt,new_borrow_flage,biz_product_type_cd,txn_desc,new_account_chinessname) like '7%' then 'zhuanzhang'
	  when my_function(payer_name,acct_num,currency_conv_type_cd,txn_cd,txn_amt,new_borrow_flage,biz_product_type_cd,txn_desc,new_account_chinessname) like '8%' then 'cunqu'
	  when my_function(payer_name,acct_num,currency_conv_type_cd,txn_cd,txn_amt,new_borrow_flage,biz_product_type_cd,txn_desc,new_account_chinessname) like '9%' then 'other'
	  end as transtype,
	  cast(txn_amt as decimal ) as amount,
	   et
	  from source_table_1;

insert into sink_table_1
select
    count (event_id) as xiaofei_01 over








select
    ttype,
    count(eid) over w as xiaofei_ccount,
    sum(amount) over w as xiaofei_aamount
    from (
            select
                cast(event_id as string) eid,
                case
                when cast(rulecode as string) like '1%' then 'xiaofei'
                when cast(rulecode as string) like '2%' then 'bangong'
                when cast(rulecode as string) like '3%' then 'xinzi'
                when cast(rulecode as string) like '4%' then 'baoxian'
                when cast(rulecode as string) like '5%' then 'touzi'
                when cast(rulecode as string) like '6%' then 'rongzi'
                when cast(rulecode as string) like '7%' then 'zhuanzhang'
                when cast(rulecode as string) like '8%' then 'cunqu'
                when cast(rulecode as string) like '9%' then 'other'
                end as ttype,
                amount,
                ts from source_table
    )t1 where ttype='xiaofei'
    window w as (order by ts range between interval '20' second preceding and current row)
union
select
    ttype,
    count(eid) over w as bangong_ccount,
    sum(amount) over w as bangong_aamount
    from (
            select
                cast(event_id as string) eid,
                case
                when cast(rulecode as string) like '1%' then 'xiaofei'
                when cast(rulecode as string) like '2%' then 'bangong'
                when cast(rulecode as string) like '3%' then 'xinzi'
                when cast(rulecode as string) like '4%' then 'baoxian'
                when cast(rulecode as string) like '5%' then 'touzi'
                when cast(rulecode as string) like '6%' then 'rongzi'
                when cast(rulecode as string) like '7%' then 'zhuanzhang'
                when cast(rulecode as string) like '8%' then 'cunqu'
                when cast(rulecode as string) like '9%' then 'other'
                end as ttype,
                amount,
                ts from source_table
    )t1 where ttype='bangong'
    window w as (order by ts range between interval '20' second preceding and current row)




























