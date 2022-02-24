package com.enbd.flink.state;

import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.kafka.KafkaCheckpointMark;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;

public class StateTypeProvider {

    public static CoderTypeInformation<KV<? extends UnboundedSource<KafkaRecord<String, String>, KafkaCheckpointMark>, KafkaCheckpointMark>>
            KAFKA_OPERATOR_STATE_TYPE;

    static{
        StateTypeProvider stateTypeProvider = new StateTypeProvider();
        KAFKA_OPERATOR_STATE_TYPE = stateTypeProvider.getKafkaReadOperatorStateType();
    }

    public CoderTypeInformation<KV<? extends UnboundedSource<KafkaRecord<String, String>, KafkaCheckpointMark>, KafkaCheckpointMark>>
    getKafkaReadOperatorStateType(){
        //Create in constructor to cache the type informations
        PipelineOptions options = PipelineOptionsFactory.fromArgs().as(PipelineOptions.class);

        Coder<KafkaCheckpointMark> checkpointMarkCoder = AvroCoder.of(KafkaCheckpointMark.class);
        Coder<? extends UnboundedSource<KafkaRecord<String, String>, KafkaCheckpointMark>> sourceCoder =
                (Coder) SerializableCoder.of(new TypeDescriptor<UnboundedSource>() {});

        KvCoder<? extends UnboundedSource<KafkaRecord<String, String>, KafkaCheckpointMark>, KafkaCheckpointMark>
                checkpointCoder = KvCoder.of(sourceCoder, checkpointMarkCoder);

        @SuppressWarnings("unchecked")
        CoderTypeInformation<KV<? extends UnboundedSource<KafkaRecord<String, String>, KafkaCheckpointMark>, KafkaCheckpointMark>>
                typeInformation =
                (CoderTypeInformation)
                        new CoderTypeInformation<>(checkpointCoder, options);

        return typeInformation;
    }
}
