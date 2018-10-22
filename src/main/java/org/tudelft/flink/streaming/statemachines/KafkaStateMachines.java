package org.tudelft.flink.streaming.statemachines;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.tudelft.flink.streaming.statemachines.helpers.SymbolConfig;

import java.util.LinkedList;
import java.util.Queue;

public class KafkaStateMachines {

    public static void main(String[] args) throws Exception {
        //test();

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
        //env.enableCheckpointing(3600000, CheckpointingMode.EXACTLY_ONCE);
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.getConfig().setLatencyTrackingInterval(1L);

        // create Kafka consumer
        FlinkKafkaConsumer010<StateMachineNetFlow> kafkaConsumer = new FlinkKafkaConsumer010<>(
                parameterTool.getRequired("topic"),
                new StateMachineNetFlowSchema(),
                parameterTool.getProperties());


        // create stream
        DataStream<StateMachineNetFlow> netFlowStream = env.addSource(kafkaConsumer);

        // real-time learning/detection
        DataStream<StateMachineNetFlow> hostSequences = netFlowStream
                .flatMap(new FlatMapFunction<StateMachineNetFlow, StateMachineNetFlow>() {
                    @Override
                    public void flatMap(StateMachineNetFlow in, Collector<StateMachineNetFlow> out) {
                        for (StateMachineNetFlow flow : in.dataset) {
                            out.collect(flow);
                        }
                    }
                })
                .keyBy("IPPairProtocol")
                //.timeWindow(Time.seconds(600))
                .reduce(new ReduceFunction<StateMachineNetFlow>() {
                    @Override
                    public StateMachineNetFlow reduce(StateMachineNetFlow rollingCount, StateMachineNetFlow newNetFlow) {
                        rollingCount.consumeElement(newNetFlow);
                        return rollingCount;
                    }
                });

        /*
        VisualisePAutomac vis = new VisualisePAutomac();
        String path = "input\\pautomac\\validation\\set10\\0-1pautomac_model.txt";
        vis.visualise(path);

        if (true) {
            return;
        }
        */

        /*
        // PERFORMANCE EVALUATION - PAUTOMAC
        // learn statemachines
        DataStream<StateMachineNetFlow> datasetStateMachines = netFlowStream
                .flatMap(new FlatMapFunction<StateMachineNetFlow, StateMachineNetFlow>() {
                    @Override
                    public void flatMap(StateMachineNetFlow in, Collector<StateMachineNetFlow> out) {
                        for (StateMachineNetFlow flow : in.dataset) {
                            out.collect(flow);
                        }
                    }
                })
                .keyBy("datasetLabel")
                .reduce(new ReduceFunction<StateMachineNetFlow>() {
                    @Override
                    public StateMachineNetFlow reduce(StateMachineNetFlow rollingStateMachine, StateMachineNetFlow nextElement) {
                        rollingStateMachine.consumeElement(nextElement);
                        return rollingStateMachine;
                    }
                });
        */

        // trigger execution
        env.execute("Kafka NetFlow StateMachines");
    }


    /**
     * Abstract class for a custom user configuration object registered at the execution config.
     *
     * This user config is accessible at runtime through
     * getRuntimeContext().getExecutionConfig().GlobalJobParameters()
     */
    public static class JobParameters extends ExecutionConfig.GlobalJobParameters {
        public SymbolConfig config;
    }

    public static void test() {
        State s1 = new State(State.Color.BLUE, 1);
        State s2 = new State(State.Color.BLUE, 1);

        Queue<Symbol> f = new LinkedList<>();
        f.add(new Symbol("1"));
        f.add(new Symbol("1"));
        f.add(new Symbol("1"));
        s1.increaseFrequency(f);
        s2.increaseFrequency(f);

        f = new LinkedList<>();
        f.add(new Symbol("1"));
        f.add(new Symbol("0"));
        f.add(new Symbol("1"));
        s1.increaseFrequency(f);
        s2.increaseFrequency(f);

        f = new LinkedList<>();
        f.add(new Symbol("1"));
        f.add(new Symbol("0"));
        f.add(new Symbol("1"));
        s1.increaseFrequency(f);
        //s2.increaseFrequency(f);

        float[] sk1 = s1.getSketchVector();
        for (int i = 0; i < sk1.length; i++) {
            System.out.print(sk1[i] + ",");
        }

        System.out.println("");

        float[] sk2 = s2.getSketchVector();
        for (int i = 0; i < sk2.length; i++) {
            System.out.print(sk2[i] + ",");
        }

        System.out.println("");

        System.out.println(s1.similarTo(s2));

        System.exit(1);
    }

}