package com.enbd.flink.state.operator;

import com.enbd.flink.state.StateTypeProvider;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.kafka.KafkaCheckpointMark;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.state.api.functions.StateBootstrapFunction;

import java.util.List;
import java.util.stream.Collectors;

public class DummyBootstrapFunction extends StateBootstrapFunction<String> {

    @Override
    public void processElement(String s, Context context) throws Exception {

    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
