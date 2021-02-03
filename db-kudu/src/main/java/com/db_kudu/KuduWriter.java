package com.db_kudu;

import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class KuduWriter<T> implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final KuduTableInfo tableInfo;
    private final KuduWriterConfig writerConfig;
    private final KuduFailureHandler failureHandler;
    private final KuduOperationMapper<T> operationMapper;

    private transient KuduClient client;
    private transient KuduSession session;
    private transient KuduTable table;

    public KuduWriter(KuduTableInfo tableInfo, KuduWriterConfig writerConfig, KuduOperationMapper<T> operationMapper) throws IOException {
        this(tableInfo, writerConfig, operationMapper, new DefaultKuduFailureHandler());
    }

    public KuduWriter(KuduTableInfo tableInfo, KuduWriterConfig writerConfig, KuduOperationMapper<T> operationMapper, KuduFailureHandler failureHandler) throws IOException {
        this.tableInfo = tableInfo;
        this.writerConfig = writerConfig;
        this.failureHandler = failureHandler;

        this.client = obtainClient();
        this.session = obtainSession();
        this.table = obtainTable();
        this.operationMapper = operationMapper;
    }

    private KuduClient obtainClient() {
        return new KuduClient.KuduClientBuilder(writerConfig.getMasters()).build();
    }

    private KuduSession obtainSession() {
        KuduSession session = client.newSession();
        session.setFlushMode(writerConfig.getFlushMode());
        return session;
    }

    private KuduTable obtainTable() throws IOException {
        String tableName = tableInfo.getName();
        if (client.tableExists(tableName)) {
            return client.openTable(tableName);
        }
        if (tableInfo.getCreateTableIfNotExists()) {
            return client.createTable(tableName, tableInfo.getSchema(), tableInfo.getCreateTableOptions());
        }
        throw new RuntimeException("Table " + tableName + " does not exist.");
    }

    public void write(T input) throws IOException {
        checkAsyncErrors();

        for (Operation operation : operationMapper.createOperations(input, table)) {
            checkErrors(session.apply(operation));
        }
    }

    public void flushAndCheckErrors() throws IOException {
        checkAsyncErrors();
        session.flush();
        checkAsyncErrors();
    }


    @Override
    public void close() throws Exception {
        try {
            flushAndCheckErrors();
        } finally {
            try {
                if (session != null) {
                    session.close();
                }
            } catch (Exception e) {
                log.error("Error while closing session.", e);
            }
            try {
                if (client != null) {
                    client.close();
                }
            } catch (Exception e) {
                log.error("Error while closing client.", e);
            }
        }
    }

    private void checkErrors(OperationResponse response) throws IOException {
        if (response != null && response.hasRowError()) {
            failureHandler.onFailure(Arrays.asList(response.getRowError()));
        } else {
            checkAsyncErrors();
        }
    }

    private void checkAsyncErrors() throws IOException {
        if (session.countPendingErrors() == 0) {
            return;
        }

        List<RowError> errors = Arrays.asList(session.getPendingErrors().getRowErrors());
        failureHandler.onFailure(errors);
    }

}