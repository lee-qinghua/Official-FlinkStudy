/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.otis.work.date20200925解析json;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.StreamTableDescriptor;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		EnvironmentSettings settings= EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

		String name            = "myhive";
		String defaultDatabase = "flink";
		String hiveConfDir     = "D:\\HadoopConfig"; // a local path
		String version         = "1.1.0";
		HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
		tableEnvironment.registerCatalog("myhive", hive);
		tableEnvironment.useCatalog("myhive");


        StreamTableDescriptor connect = tableEnvironment.connect(new FileSystem()
                .path("file:///D:\\peoject\\Official-FlinkStudy\\flink-1.11\\src\\main\\java\\com\\otis\\work\\date20200925解析json\\c.json"));
        hive.createTable(
                new ObjectPath("flink", "liqinghua_test1"),
                new CatalogTableImpl(
                        sql_schema.schema,
                        connect.toProperties(),
                        "my comment"
                ),true);


//        hive.createTable(
//                new ObjectPath("flink", "sgs_test"),
//                new CatalogTableImpl(
//                        sql_schema.schema,
//                        connect.toProperties(),
//                        "my comment"
//        ),true);
//
//
//        ConnectTableDescriptor sgs_properties = tableEnvironment.connect(new Kafka()
//				.version("universal")
//				.topic("aaatest")
//				.startFromLatest()
//				.property("group.id", "test")
//				.property("zookeeper.connect", "real-time-002:2181,real-time-003:2181,real-time-004:2181")
//				.property("bootstrap.servers", "real-time-002:9092,real-time-003:9092,real-time-004:9092"))
//				.withFormat(new Json().failOnMissingField(true).deriveSchema())
//				.inAppendMode();
//
//		hive.createTable(
//				new ObjectPath("flink", "sgs_test"),
//				new CatalogTableImpl(
//						sql_schema.schema,
//						sgs_properties.toProperties(),
//						"my comment"
//				),
//				true);
//
//		//企业状态所在表 entinvs 需要的字段 entstatus
//		Table entinvs = tableEnvironment.sqlQuery("select sgs_test.creditcode as creditcode,sgs_test.reccap  as reccap, t.entstatus  from sgs_test, unnest(sgs_test.entinvs) as  t(esdate,regno,creditcode,enttype,entjgname,binvvamount,subconam,regcapcur,candate,conform,regcap,name,regorg,congrocur,entstatus,fundedratio,revdate,regorgcode)");
//		tableEnvironment.createTemporaryView("entinvs",entinvs);
//
//		//处罚决定文书所在表 entcasebaseinfos  需要的字段 pendecno
//		Table entcasebaseinfos = tableEnvironment.sqlQuery("select sgs_test.creditcode as creditcode, t.pendecno  from sgs_test, unnest(sgs_test.entcasebaseinfos) as  t(illegacttype,pendecno,pencontent,pentypecn,pentype,publicdate,penauthcn,penauth,pendecissdate)");
//		tableEnvironment.createTemporaryView("entcasebaseinfos",entcasebaseinfos);
//
//		//认缴出资额（万元）所在表 shareholders 需要的字段 subconam
//		Table shareholders = tableEnvironment.sqlQuery("select sgs_test.creditcode as creditcode, t.subconam  from sgs_test, unnest(sgs_test.shareholders) as  t(country,invtype,conform,shaname,fundedratio,subconam,condate,regcapcur)");
//		tableEnvironment.createTemporaryView("shareholders",shareholders);
//
//		//出质股权数额  所在表 stockpawns 需要的字段 stkpawnczamt
//		Table stockpawns = tableEnvironment.sqlQuery("select sgs_test.creditcode as creditcode,t.stkpawnczamt  as stkpawnczamt   from sgs_test, unnest(sgs_test.stockpawns) as  t(stkpawnregno,stkpawnczamt,stkpawnczcerno,stkpawnzqcerno,stkpawndate,stkpawnstatus,stkpawnczper,stkpawnregdate,stkpawnzqper,url)");
//		tableEnvironment.createTemporaryView("stockpawns",stockpawns);
//
//		//剩下的指标  所在的表 yearReportSocSecs
//		Table yearReportSocSecs = tableEnvironment.sqlQuery("select sgs_test.creditcode as creditcode, t.so110,t.so210,t.so310,t.so410,t.so510,t.totalpaymentso110,t.totalpaymentso210,t.totalpaymentso310   from sgs_test, unnest(sgs_test.yearReportSocSecs) as  t(unpaidsocialinsso110,unpaidsocialinsso210,unpaidsocialinsso310,unpaidsocialinsso410,unpaidsocialinsso510,totalwagesso110,totalwagesso310,totalwagesso210,so410,so310,so210,so110,totalwagesso510,totalwagesso410,totalpaymentso210,totalpaymentso310,totalpaymentso110,totalpaymentso410,totalpaymentso510,ancheid,so510)");
//		tableEnvironment.createTemporaryView("yearReportSocSecs",yearReportSocSecs);
//
//		//Table table = tableEnvironment.sqlQuery("select sgs_test.creditcode,sum(CAST(stkpawnczamt AS DOUBLE)) from sgs_test inner join stockpawns on 1=1  group by sgs_test.creditcode");
//
//		//Table table = tableEnvironment.sqlQuery("select*from stockpawns");
//
//		//注册kafkaSink
//
//		tableEnvironment.sqlUpdate("CREATE TABLE kafkaSink (\n" +
//				"creditcode VARCHAR," +
//				"GSY_FEATURE_1 VARCHAR," +
//				"GSY_FEATURE_2 VARCHAR," +
//				"GSY_FEATURE_3 VARCHAR," +
//				"GSY_FEATURE_5 VARCHAR," +
//				"GSY_FEATURE_7 VARCHAR," +
//				"GSY_FEATURE_8 VARCHAR," +
//				"GSY_FEATURE_9 VARCHAR," +
//				"GSY_FEATURE_10 VARCHAR," +
//				"GSY_FEATURE_11 VARCHAR," +
//				"GSY_FEATURE_12 VARCHAR," +
//				"GSY_FEATURE_13 VARCHAR," +
//				"GSY_FEATURE_14 VARCHAR," +
//				"GSY_FEATURE_15 VARCHAR"+
//				"            ) WITH (\n" +
//				"               'connector.type' = 'kafka',\n" +
//				"                'connector.version' = 'universal',\n" +
//				"               'connector.topic' = 'tttest',\n" +
//				"                'connector.startup-mode' = 'latest-offset',\n" +
//				"                'connector.properties.zookeeper.connect' = 'real-time-002:2181,real-time-003:2181,real-time-004:2181',\n" +
//				"                'connector.properties.bootstrap.servers' = 'real-time-002:9092,real-time-003:9092,real-time-004:9092',\n" +
//				"                'connector.properties.group.id' = 'test_demo',\n" +
//				"                'update-mode' = 'append',\n" +
//				"                'format.type' = 'json',\n" +
//				"                'format.derive-schema' = 'true'\n" +
//				"            )");
//
//		tableEnvironment.sqlUpdate("insert into kafkaSink select entinvs.creditcode as creditcode,reccap as GSY_FEATURE_1," +
//				"entstatus as GSY_FEATURE_2,pendecno as GSY_FEATURE_3,subconam as GSY_FEATURE_5,stkpawnczamt as GSY_FEATURE_7,yearReportSocSecs.so110 as GSY_FEATURE_8," +
//				"yearReportSocSecs.so210 as GSY_FEATURE_9,yearReportSocSecs.so310 as GSY_FEATURE_10," +
//				"yearReportSocSecs.so410 as GSY_FEATURE_11,yearReportSocSecs.so510 as GSY_FEATURE_12," +
//				"yearReportSocSecs.totalpaymentso110 as GSY_FEATURE_13,yearReportSocSecs.totalpaymentso210 as GSY_FEATURE_14," +
//				"yearReportSocSecs.totalpaymentso310 as GSY_FEATURE_15  from entinvs inner join" +
//				" entcasebaseinfos on entinvs.creditcode=entcasebaseinfos.creditcode" +
//				" inner join shareholders on shareholders.creditcode=entinvs.creditcode " +
//				"inner join stockpawns on stockpawns.creditcode=entinvs.creditcode" +
//				" inner join yearReportSocSecs on yearReportSocSecs.creditcode=entinvs.creditcode");

		tableEnvironment.execute("StreamingJob");
	}
}
