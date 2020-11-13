package com.work.cassandra;

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Collector;

public class ReadCassandraFlatMap extends RichFlatMapFunction<FlinkReadCassandra.Stu, FlinkReadCassandra.Student> {

    private ClusterBuilder builder = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        builder = new ClusterBuilder() {
            @Override
            protected Cluster buildCluster(Cluster.Builder builder) {
                return builder.addContactPoint("10.1.30.10").withPort(9042).build();
            }
        };
    }

    @Override
    public void flatMap(FlinkReadCassandra.Stu stu, Collector<FlinkReadCassandra.Student> collector) throws Exception {
        String sql = "select * from flink.score where id='" + stu.id() + "';";
        CassandraInputFormat<Tuple2<String,Integer>> format = new CassandraInputFormat<>(sql, builder);
        format.configure(null);
        format.open(null);
        Tuple2<String,Integer> testOutputTuple = new Tuple2<>();
        Tuple2<String,Integer> ret = format.nextRecord(testOutputTuple);
        collector.collect(new FlinkReadCassandra.Student(stu.id(), stu.name(), stu.age(), ret.f1));
    }

    @Override
    public void close() throws Exception {
        builder.getCluster().close();
        super.close();
    }
}
