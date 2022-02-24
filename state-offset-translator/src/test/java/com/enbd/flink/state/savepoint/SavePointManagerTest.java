package com.enbd.flink.state.savepoint;

import com.enbd.flink.state.StateTypeProvider;
import com.enbd.flink.state.operator.DefaultOperatorExtractor;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.kafka.KafkaCheckpointMark;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class SavePointManagerTest {

    //Test no kafka operator
    @Test
    public void switchOverShouldCreateIdenticalSavePointIfNoKafkaStateExists() throws Exception {
        //arrange
        SavePointManager savePointManager = new SavePointManager();
        String existingSavePointPath = getResourcePath("FSNoKafkaOperatorState");
        String newSavePointPath = getOutputSavepintPath("FSSavepointWithoutKafkaOperatorState");
        deleteDirectory(new File(newSavePointPath));
        //act
        boolean result = savePointManager.createSwitchOverSavePoint(
                "file://" +existingSavePointPath,
                "file://" +newSavePointPath);

        //assert
        Assert.assertTrue("New Savepoint should be created",result);

        ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(env, "file://" + newSavePointPath,
                new FsStateBackend("file://" +newSavePointPath));

        Assert.assertEquals("Number of operators should increase by one dummy operator",
                8,savepoint.getSavepointMetadata().getExistingOperators().size());
    }


    //Test Single Kafka Operators
    @Test
    public void switchOverSavePointShouldResetSingleKafkaOperatorState() throws Exception {
        //arrange
        SavePointManager savePointManager = new SavePointManager();
        String existingSavePointPath = getResourcePath("FSSingleKafkaOperatorState");
        String newSavePointPath = getOutputSavepintPath("FSSavePointShouldRemoveKafkaOperatorState");
        deleteDirectory(new File(newSavePointPath));
        //act
        boolean result = savePointManager.createSwitchOverSavePoint(
                "file://" + existingSavePointPath,
                "file://" +newSavePointPath);

        //assert
        Assert.assertTrue("New Savepoint should be created",result);

        ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(env, "file://" + newSavePointPath,
                new FsStateBackend("file://" +newSavePointPath));

        Assert.assertEquals("Number of operators should not change",
                7,savepoint.getSavepointMetadata().getExistingOperators().size());

        DefaultOperatorExtractor defaultOperatorExtractor = new DefaultOperatorExtractor();
        List<OperatorState> kafkaOperators = defaultOperatorExtractor.extractSavePointOperators(savepoint.getSavepointMetadata());

        Assert.assertEquals("Number of Kafka operators should not change",1,kafkaOperators.size());
        Assert.assertEquals("Parallelism of Kafka operators should not change",3,kafkaOperators.get(0).getParallelism());


        List<KV<? extends UnboundedSource<KafkaRecord<String, String>, KafkaCheckpointMark>, KafkaCheckpointMark>> stateList =
                savepoint.readListState(kafkaOperators.get(0).getOperatorID(),
                        DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME,
                        StateTypeProvider.KAFKA_OPERATOR_STATE_TYPE)
                        .collect();

        Assert.assertEquals("Number of Kafka states should not change",3,stateList.size());
        for (KafkaCheckpointMark checkpointMark :
                stateList.stream().map(s -> s.getValue()).collect(Collectors.toList())) {
            Assert.assertEquals("Number of Kafka partitions should not change",1,checkpointMark.getPartitions().size());
            Assert.assertEquals("Offset should be reset",-1,checkpointMark.getPartitions().get(0).getNextOffset());
        }

    }

    //TestMultiple Kafka Operators
    @Test
    public void switchOverSavePointShouldResetMultipleKafkaOperatorsState() throws Exception {
        //arrange
        SavePointManager savePointManager = new SavePointManager();
        String existingSavePointPath = getResourcePath("FSMultipleKafkaOperatorsState");
        String newSavePointPath = getOutputSavepintPath("FSSavePointShouldRemoveMultipleKafkaOperatorsState");
        deleteDirectory(new File(newSavePointPath));
        //act
        boolean result = savePointManager.createSwitchOverSavePoint(
                "file://" + existingSavePointPath,
                "file://" +newSavePointPath);

        //assert
        Assert.assertTrue("New Savepoint should be created",result);

        ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(env, "file://" + newSavePointPath,
                new FsStateBackend("file://" +newSavePointPath));

        Assert.assertEquals("Number of operators should not change",
                13,savepoint.getSavepointMetadata().getExistingOperators().size());

        DefaultOperatorExtractor defaultOperatorExtractor = new DefaultOperatorExtractor();
        List<OperatorState> kafkaOperators = defaultOperatorExtractor.extractSavePointOperators(savepoint.getSavepointMetadata());

        Assert.assertEquals("Number of Kafka operators should not change",2,kafkaOperators.size());
        Assert.assertEquals("Parallelism of Kafka operators should not change",3,kafkaOperators.get(0).getParallelism());

    }

    @Test
    public void switchOverRocksDbSavePointShouldResetMultipleKafkaOperatorsState() throws Exception {
        //arrange
        SavePointManager savePointManager = new SavePointManager();
        String existingSavePointPath = getResourcePath("RDBMultipleKafkaOperatorsState");
        String newSavePointPath = getOutputSavepintPath("RDBSavePointShouldRemoveMultipleKafkaOperatorsState");
        deleteDirectory(new File(newSavePointPath));
        //act
        boolean result = savePointManager.createSwitchOverSavePoint(
                "file://" + existingSavePointPath,
                "file://" +newSavePointPath);

        //assert
        Assert.assertTrue("New Savepoint should be created",result);

        ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(env, "file://" + newSavePointPath,
                new FsStateBackend("file://" +newSavePointPath));

        Assert.assertEquals("Number of operators should not change",
                13,savepoint.getSavepointMetadata().getExistingOperators().size());

        DefaultOperatorExtractor defaultOperatorExtractor = new DefaultOperatorExtractor();
        List<OperatorState> kafkaOperators = defaultOperatorExtractor.extractSavePointOperators(savepoint.getSavepointMetadata());

        Assert.assertEquals("Number of Kafka operators should not change",2,kafkaOperators.size());
        Assert.assertEquals("Parallelism of Kafka operators should not change",3,kafkaOperators.get(0).getParallelism());

    }

    private String getResourcePath(String resourcePath){
        Path resourceDirectory = Paths.get("src","test","resources",resourcePath);
        String absolutePath = resourceDirectory.toFile().getAbsolutePath();
        return absolutePath;
    }

    private String getOutputSavepintPath(String targetPath){
        Path resourceDirectory = Paths.get("target","outputSavePoints",targetPath);
        String absolutePath = resourceDirectory.toFile().getAbsolutePath();
        return absolutePath;
    }

    private boolean deleteDirectory(File directoryToBeDeleted) {

        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }
}
