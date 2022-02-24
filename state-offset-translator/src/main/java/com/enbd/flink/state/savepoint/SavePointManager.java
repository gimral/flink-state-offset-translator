package com.enbd.flink.state.savepoint;

import com.enbd.flink.state.operator.DummyOperatorTransformation;
import com.enbd.flink.state.operator.DefaultOperatorExtractor;
import com.enbd.flink.state.operator.OffsetResetBootstrapFunction;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.kafka.KafkaCheckpointMark;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.enbd.flink.state.StateTypeProvider;

import java.util.List;

public class SavePointManager {


    private static Logger logger = LoggerFactory.getLogger(SavePointManager.class);

    private final DefaultOperatorExtractor kafkaOperatorExtractor;
    private final StateTypeProvider stateTypeProvider;
    private final DummyOperatorTransformation dummyOperatorTransformation;

    public SavePointManager() {
        kafkaOperatorExtractor = new DefaultOperatorExtractor();
        stateTypeProvider = new StateTypeProvider();
        dummyOperatorTransformation = new DummyOperatorTransformation();
    }

    public boolean createSwitchOverSavePoint(String existingSavePointPath,String newSavePointPath) throws Exception {
        ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(env, existingSavePointPath,
                new FsStateBackend(existingSavePointPath));//.configure should be called?
        if(!containsAnyState(savepoint))
            return false;
        //Assuming every operator uses same parallelism.Beam should be working like that
        env.setParallelism(savepoint.getSavepointMetadata().getExistingOperators().get(0).getParallelism());

        boolean modified = resetKafkaOperatorStates(savepoint);
        if(!modified){
            savepoint.withOperator("DummySavePointOperator",dummyOperatorTransformation.create(env));
        }
        savepoint.write(newSavePointPath);
        JobExecutionResult r = env.execute();
        logger.info(r.toString());
        return true;
    }

    private boolean resetKafkaOperatorStates(ExistingSavepoint savepoint) throws Exception {
        boolean modified = false;
        List<OperatorState> kafkaOperatorStates = kafkaOperatorExtractor.extractSavePointOperators(savepoint.getSavepointMetadata());
        for (OperatorState s : kafkaOperatorStates) {
            DataSet<KV<? extends UnboundedSource<KafkaRecord<String, String>, KafkaCheckpointMark>, KafkaCheckpointMark>> stateList =
                    savepoint.readListState(s.getOperatorID(),
                    DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME,
                            StateTypeProvider.KAFKA_OPERATOR_STATE_TYPE);

            try {
                if (stateList.count() == 0)
                    continue;
            }
            //This is not Kafka Operator State
            catch (Exception ex){
                continue;
            }

            savepoint = savepoint.removeOperator(s.getOperatorID());
            BootstrapTransformation offsetTransformation = OperatorTransformation
                    .bootstrapWith(stateList)
                    .setMaxParallelism(s.getMaxParallelism())
                    .transform(new OffsetResetBootstrapFunction());

            savepoint =savepoint.withOperator(s.getOperatorID(),offsetTransformation);
            modified = true;
        }
        return modified;
    }

    private boolean containsAnyState(ExistingSavepoint savepoint){
        return !savepoint.getSavepointMetadata().getExistingOperators().isEmpty();
    }
}
