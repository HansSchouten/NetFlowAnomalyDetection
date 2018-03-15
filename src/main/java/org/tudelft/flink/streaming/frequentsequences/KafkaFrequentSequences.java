package org.tudelft.flink.streaming.frequentsequences;

import org.apache.flink.api.common.functions.ReduceFunction;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

public class KafkaFrequentSequences {

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // validate input arguments
        if(parameterTool.getNumberOfParameters() < 2) {
            System.out.println("Missing parameters!\nUsage: Kafka --topic <topic> " +
                    "--bootstrap.servers <kafka brokers> --zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }

        // setup Flink stream execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        // create a checkpoint every 5 seconds
        //env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        // create Kafka consumer
        FlinkKafkaConsumer010<FrequentSequenceNetFlow> kafkaConsumer = new FlinkKafkaConsumer010<>(
                parameterTool.getRequired("topic"),
                new FrequentSequenceNetFlowSchema(),
                parameterTool.getProperties());

        // create stream
        DataStream<FrequentSequenceNetFlow> netFlowStream = env.addSource(kafkaConsumer);

        // write Kafka stream to standard out.
        DataStream<FrequentSequenceNetFlow> hostSequences = netFlowStream
                .keyBy("srcIP")
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<FrequentSequenceNetFlow>() {
                    @Override
                    public FrequentSequenceNetFlow reduce(FrequentSequenceNetFlow rollingCount, FrequentSequenceNetFlow newNetflow) {
                        rollingCount.processFlow(newNetflow);
                        return rollingCount;
                    }
                });

        // output the results (with a single thread, rather than in parallel)
        hostSequences.print();//.setParallelism(1);

        // trigger execution
        env.execute("Kafka NetFlow FrequentSequences");
    }
}