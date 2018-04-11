package org.tudelft.flink.streaming;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class KafkaProducer {

    protected StreamExecutionEnvironment env;
    protected String[] args;

    public KafkaProducer(StreamExecutionEnvironment env, String[] args) {
        this.env = env;
        this.args = args;
    }

    public void addSourceFunction(SourceFunction<String> sourceFunction) {
        ParameterTool parameterTool = ParameterTool.fromArgs(this.args);
        // create data stream
        DataStream<String> messageStream = this.env.addSource(sourceFunction);
        // add sink to Kafka
        messageStream.addSink(new FlinkKafkaProducer010<>(parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties()));
    }

}
