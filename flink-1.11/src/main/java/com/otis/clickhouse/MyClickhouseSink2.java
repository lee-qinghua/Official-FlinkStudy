package com.otis.clickhouse;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MyClickhouseSink2 extends RichSinkFunction<Row> {
    private String sql;

    //多个节点的connection
    private List<Connection> connectionList = new ArrayList<>();

    //存放数据的集合
    private List<Row> dataList = new ArrayList<>();

    //Row 的字段和类型
    private String[] tableColums;
    private List<String> types;

    // 连接参数
    private String drivername = "ru.yandex.clickhouse.ClickHouseDriver";
    private String[] ips;
    private String username;
    private String password;
    private String database;

    private long lastInsertTime = 0L;
    // 每批插入的时间间隔
    private final long insertCkTimenterval = 1000000;
    // 插入的批次 一万条
    private final int insertCkBatchSize = 5;

    public MyClickhouseSink2(String sql, String[] tableColums, List<String> types, String[] ips, String username, String password, String database) {
        this.sql = sql;
        this.tableColums = tableColums;
        this.types = types;
        this.ips = ips;
        this.username = username;
        this.password = password;
        this.database = database;
    }

    public static void insertData(String sql, String[] tableColums, List<Row> rows, List<String> types, Connection connection) throws SQLException {
        long startTime = System.currentTimeMillis();
        if (rows.size() == 0) return;
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            for (int i1 = 0; i1 < tableColums.length; i1++) {
                if (row.getField(i1) != null) {
                    switch (types.get(i1)) {
                        case "string":
                            preparedStatement.setString(i1 + 1, (String) row.getField(i1));
                            break;
                        case "int":
                            preparedStatement.setInt(i1 + 1, (int) row.getField(i1));
                            break;
                        default:
                            break;
                    }
                } else {
                    preparedStatement.setObject(i1 + 1, null);
                }
            }
            preparedStatement.addBatch();
        }
        int[] ints = preparedStatement.executeBatch();
        connection.commit();
        long endTime = System.currentTimeMillis();
        System.out.println("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + ints.length);
    }


    public void createConnection() throws SQLException {
        for (String ip : ips) {
            String address = "jdbc:clickhouse://" + ip + ":" + "8123" + "/" + database;
            Connection connection = DriverManager.getConnection(address, username, password);
            connectionList.add(connection);
        }
    }

    private boolean isTimeToInsert() {
        long currTime = System.currentTimeMillis();
        return currTime - this.lastInsertTime >= this.insertCkTimenterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(drivername);
        createConnection();
    }

    @Override
    public void close() throws Exception {
        super.close();
        for (Connection connection : connectionList) {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Override
    public void invoke(Row row, Context context) throws Exception {
        System.out.println("执行了一次！！！！！！！！！！！！！！！！！！！！！");
        if (row != null) {
            if (dataList.size() == insertCkBatchSize || isTimeToInsert()) {
                // 轮询写入各个local表，避免单节点数据过多
                Random random = new Random();
                int i = random.nextInt(ips.length);
                Connection connection = connectionList.get(i);
                insertData(sql, tableColums, dataList, types, connection);
                dataList.clear();
                lastInsertTime = System.currentTimeMillis();
            } else {
                dataList.add(row);
            }
        }
    }
}

