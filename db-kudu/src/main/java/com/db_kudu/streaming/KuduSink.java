package com.db_kudu.streaming;

import com.db_kudu.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class KuduSink<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final KuduTableInfo tableInfo;
    private final KuduWriterConfig writerConfig;
    private final KuduFailureHandler failureHandler;
    private final KuduOperationMapper<IN> opsMapper;
    private transient KuduWriter kuduWriter;

    public KuduSink(KuduWriterConfig writerConfig, KuduTableInfo tableInfo, KuduOperationMapper<IN> opsMapper) {
        this(writerConfig, tableInfo, opsMapper, new DefaultKuduFailureHandler());
    }

    public KuduSink(KuduWriterConfig writerConfig, KuduTableInfo tableInfo, KuduOperationMapper<IN> opsMapper, KuduFailureHandler failureHandler) {
        this.tableInfo = checkNotNull(tableInfo, "tableInfo could not be null");
        this.writerConfig = checkNotNull(writerConfig, "config could not be null");
        this.opsMapper = checkNotNull(opsMapper, "opsMapper could not be null");
        this.failureHandler = checkNotNull(failureHandler, "failureHandler could not be null");
    }

    /**
     * 初始化打开连接
     *
     * @param parameters
     * @throws Exception
     */
    public void open(Configuration parameters) throws Exception {
        kuduWriter = new KuduWriter(tableInfo, writerConfig, opsMapper, failureHandler);
    }


    public void close() throws Exception {
        if (kuduWriter != null) {
            kuduWriter.close();
        }
    }


    public void invoke(IN value) throws Exception {
        try {
            kuduWriter.write(value);
        } catch (ClassCastException e) {
            failureHandler.onTypeMismatch(e);
        }
    }


    // 保存中间状态的方法
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        kuduWriter.flushAndCheckErrors();
    }

    // 初始化状态的方法/故障恢复时调用的方法
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}
