package com.otis.clickhouse;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

public class MyClickHouseSink1 extends RichSinkFunction<List<User>> {
    Connection connection = null;
    String sql;

    public MyClickHouseSink1(String sql) {
        this.sql = sql;
    }

    public MyClickHouseSink1() {
        super();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = ClickHouseUtil.getConnection("10.1.30.10", 8123, "qinghua");
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }

    @Override
    public void invoke(List<User> value, Context context) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        for (User user : value) {
            preparedStatement.setString(1, user.getName());
            preparedStatement.setString(2, user.getAddress());
            preparedStatement.setLong(3, user.getAge());
            preparedStatement.addBatch();
        }
        long startTime = System.currentTimeMillis();
        int[] ints = preparedStatement.executeBatch();
        connection.commit();
        long endTime = System.currentTimeMillis();
        System.out.println("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + ints.length);
    }

//    @Override
//    public void invoke(User value, Context context) throws Exception {
//        PreparedStatement preparedStatement = connection.prepareStatement(sql);
//        preparedStatement.setString(1, value.getName());
//        preparedStatement.setString(2, value.getAddress());
//        preparedStatement.setLong(3, value.getAge());
//        preparedStatement.addBatch();
//
//        long startTime = System.currentTimeMillis();
//        int[] ints = preparedStatement.executeBatch();
//        connection.commit();
//        long endTime = System.currentTimeMillis();
//        System.out.println("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + ints.length);
//    }
//    @Override
//    public void invoke(List<User> list, Context context) throws Exception {
//        PreparedStatement preparedStatement = connection.prepareStatement(sql);
//        for (User user : list) {
//            preparedStatement.setString(1, user.getName());
//            preparedStatement.setString(2, user.getAddress());
//            preparedStatement.setLong(3, user.getAge());
//            preparedStatement.addBatch();
//        }
//        long startTime = System.currentTimeMillis();
//        int[] ints = preparedStatement.executeBatch();
//        connection.commit();
//        long endTime = System.currentTimeMillis();
//        System.out.println("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + ints.length);
//    }
}
