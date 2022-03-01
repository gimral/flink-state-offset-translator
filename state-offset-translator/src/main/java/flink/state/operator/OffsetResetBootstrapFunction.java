package flink.state.operator;

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
import flink.state.StateTypeProvider;

import java.util.List;
import java.util.stream.Collectors;

public class OffsetResetBootstrapFunction extends StateBootstrapFunction<KV<? extends UnboundedSource<KafkaRecord<String, String>, KafkaCheckpointMark>, KafkaCheckpointMark>> {
    private static final long UNINITIALIZED_OFFSET = -1;
    private ListState<KV<? extends UnboundedSource<KafkaRecord<String, String>, KafkaCheckpointMark>, KafkaCheckpointMark>> state;

    @Override
    public void processElement(KV<? extends UnboundedSource<KafkaRecord<String, String>, KafkaCheckpointMark>, KafkaCheckpointMark> value, Context ctx) throws Exception {
        List<KafkaCheckpointMark.PartitionMark> partitionMarks = value.getValue().getPartitions().stream()
                        .map(p -> new KafkaCheckpointMark.PartitionMark(p.getTopic(),p.getPartition(),UNINITIALIZED_OFFSET,p.getWatermarkMillis()))
                .collect(Collectors.toList());
        state.add(KV.of(value.getKey(),new KafkaCheckpointMark(partitionMarks,null)) );
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        StateTypeProvider stateTypeProvider = new StateTypeProvider();
        state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
                DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME,
                stateTypeProvider.getKafkaReadOperatorStateType()));
    }
}
