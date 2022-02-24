package com.enbd.flink.state.operator;

import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadata;
import com.enbd.flink.state.StateTypeProvider;

import java.util.List;
import java.util.stream.Collectors;

public class DefaultOperatorExtractor {
    public List<OperatorState> extractSavePointOperators(SavepointMetadata savepointMetadata){
        return savepointMetadata.getExistingOperators()
                .stream()
                .filter(this::isDefaultOperatorState)
                .collect(Collectors.toList());
    }

    private boolean isDefaultOperatorState(OperatorState state){
        return state.getStates()
                .stream()
                .anyMatch(this::containsDefaultOperatorState);
    }

    private boolean containsDefaultOperatorState(OperatorSubtaskState operatorSubtaskState){
        return operatorSubtaskState.getManagedOperatorState()
                .stream()
                .anyMatch(operatorStateHandle ->
                        operatorStateHandle.getStateNameToPartitionOffsets().containsKey(DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME) &&
                                operatorStateHandle.getStateNameToPartitionOffsets().size() == 1);
    }
}
