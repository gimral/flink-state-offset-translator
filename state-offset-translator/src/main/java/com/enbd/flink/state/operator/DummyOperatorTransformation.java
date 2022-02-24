package com.enbd.flink.state.operator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.OperatorTransformation;

public class DummyOperatorTransformation {
    public BootstrapTransformation create(ExecutionEnvironment env){
        DataSet<String> emptyset = env.fromElements("Dummy");
        return OperatorTransformation.bootstrapWith(emptyset).transform(new DummyBootstrapFunction());
    }
}
