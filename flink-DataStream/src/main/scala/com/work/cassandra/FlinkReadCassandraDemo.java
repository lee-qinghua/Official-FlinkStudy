package com.work.cassandra;

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.io.IOException;

public class FlinkReadCassandraDemo {
    public static void main(String[] args) throws IOException {
        ClusterBuilder builder = new ClusterBuilder() {

            protected Cluster buildCluster(Cluster.Builder builder) {
                return builder.addContactPoint("10.1.30.10").withPort(9042).build();
            }
        };
        CassandraInputFormat<Tuple2<String, Integer>> format = new CassandraInputFormat<>("select * from flink.wc;", builder);
        format.configure(null);
        format.open(null);

        Tuple2<String, Integer> testOutputTuple = new Tuple2<>();
        format.nextRecord(testOutputTuple);

        System.out.println("column1: " + testOutputTuple.f0);
        System.out.println("column2: " + testOutputTuple.f1);
    }
}
