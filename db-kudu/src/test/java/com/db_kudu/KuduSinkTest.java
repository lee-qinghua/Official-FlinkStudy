package com.db_kudu;

import com.db_kudu.streaming.KuduSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.types.Row;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.db_kudu.KuduTestBase.booksDataRow;
import static com.db_kudu.KuduTestBase.harness;

public class KuduSinkTest {
    private static final Logger LOG = LoggerFactory.getLogger(KuduSinkTest.class);
    private static final String[] columns = new String[]{"id", "uuid"};
    private static StreamingRuntimeContext context;

    @BeforeAll
    static void start() {
        context = Mockito.mock(StreamingRuntimeContext.class);
        Mockito.when(context.isCheckpointingEnabled()).thenReturn(true);
    }

    // 不设置master
    @Test
    void testInvalidKuduMaster() {
        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), false);
        Assertions.assertThrows(NullPointerException.class, () -> new KuduSink<>(null, tableInfo, new RowOperationMapper(columns, AbstractSingleOperationMapper.KuduOperation.INSERT)));
    }

    // 不设置tableinfo
    @Test
    void testInvalidTableInfo() {
        harness.getClient();
        String masterAddresses = harness.getMasterAddressesAsString();
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(masterAddresses).build();
        Assertions.assertThrows(NullPointerException.class, () -> new KuduSink<>(writerConfig, null, new RowOperationMapper(columns, AbstractSingleOperationMapper.KuduOperation.INSERT)));
    }

    @Test
    void testNotTableExist() {
        String masterAddresses = "real-time-006";
        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), false);
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(masterAddresses).setEventualConsistency().build();

        KuduSink<Row> sink = new KuduSink<>(writerConfig, tableInfo, new RowOperationMapper(columns, AbstractSingleOperationMapper.KuduOperation.INSERT));

        sink.setRuntimeContext(context);
        Assertions.assertThrows(RuntimeException.class, () -> sink.open(new Configuration()));
    }

    @Test
    void testOutputWithStrongConsistency() throws Exception {
        String masterAddresses = harness.getMasterAddressesAsString();

        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), true);
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder
                .setMasters(masterAddresses)
                .setStrongConsistency()
                .build();
        KuduSink<Row> sink = new KuduSink<>(writerConfig, tableInfo, new RowOperationMapper(KuduTestBase.columns, AbstractSingleOperationMapper.KuduOperation.INSERT));

        sink.setRuntimeContext(context);
        sink.open(new Configuration());

        for (Row kuduRow : booksDataRow()) {
            sink.invoke(kuduRow);
        }
        sink.close();

//        List<Row> rows = readRows(tableInfo);
//        Assertions.assertEquals(5, rows.size());
//        kuduRowsTest(rows);
    }
    @Test
    void testOutputWithEventualConsistency() throws Exception {
        String masterAddresses = harness.getMasterAddressesAsString();

        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), true);
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder
                .setMasters(masterAddresses)
                .setEventualConsistency()
                .build();
        KuduSink<Row> sink = new KuduSink<>(writerConfig, tableInfo, new RowOperationMapper(KuduTestBase.columns, AbstractSingleOperationMapper.KuduOperation.INSERT));

        sink.setRuntimeContext(context);
        sink.open(new Configuration());

        for (Row kuduRow : booksDataRow()) {
            sink.invoke(kuduRow);
        }

        // sleep to allow eventual consistency to finish
        Thread.sleep(1000);

        sink.close();

//        List<Row> rows = readRows(tableInfo);
//        Assertions.assertEquals(5, rows.size());
//        kuduRowsTest(rows);
    }

    @Test
    void testSpeed() throws Exception {
        String masterAddresses = harness.getMasterAddressesAsString();

        KuduTableInfo tableInfo = KuduTableInfo
                .forTable("test_speed")
                .createTableIfNotExists(
                        () ->
                                Lists.newArrayList(
                                        new ColumnSchema
                                                .ColumnSchemaBuilder("id", Type.INT32)
                                                .key(true)
                                                .build(),
                                        new ColumnSchema
                                                .ColumnSchemaBuilder("uuid", Type.STRING)
                                                .build()
                                ),
                        () -> new CreateTableOptions()
                                .setNumReplicas(3)
                                .addHashPartitions(Lists.newArrayList("id"), 6));

        KuduWriterConfig writerConfig = KuduWriterConfig.Builder
                .setMasters(masterAddresses)
                .setEventualConsistency()
                .build();
        KuduSink<Row> sink = new KuduSink<>(writerConfig, tableInfo, new RowOperationMapper(columns, AbstractSingleOperationMapper.KuduOperation.INSERT));

        sink.setRuntimeContext(context);
        sink.open(new Configuration());

        int totalRecords = 100000;
        for (int i = 0; i < totalRecords; i++) {
            Row kuduRow = new Row(2);
            kuduRow.setField(0, i);
            kuduRow.setField(1, UUID.randomUUID().toString());
            sink.invoke(kuduRow);
        }

        // sleep to allow eventual consistency to finish
        Thread.sleep(1000);

        sink.close();

//        List<Row> rows = readRows(tableInfo);
//        Assertions.assertEquals(totalRecords, rows.size());
    }

    public static KuduTableInfo booksTableInfo(String tableName, boolean createIfNotExist) {

        KuduTableInfo tableInfo = KuduTableInfo.forTable(tableName);

        if (createIfNotExist) {
            ColumnSchemasFactory schemasFactory = () -> {
                List<ColumnSchema> schemas = new ArrayList<>();
                schemas.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
                schemas.add(new ColumnSchema.ColumnSchemaBuilder("title", Type.STRING).build());
                schemas.add(new ColumnSchema.ColumnSchemaBuilder("author", Type.STRING).build());
                schemas.add(new ColumnSchema.ColumnSchemaBuilder("price", Type.DOUBLE).nullable(true).build());
                schemas.add(new ColumnSchema.ColumnSchemaBuilder("quantity", Type.INT32).nullable(true).build());
                return schemas;
            };

            tableInfo.createTableIfNotExists(
                    schemasFactory,
                    () -> new CreateTableOptions()
                            .setNumReplicas(1)
                            .addHashPartitions(Lists.newArrayList("id"), 2));
        }

        return tableInfo;
    }
}
