package org.tudelft.flink.streaming.statemachines;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.tudelft.flink.streaming.KafkaProducer;
import org.tudelft.flink.streaming.NetFlowReader;
import org.tudelft.flink.streaming.statemachines.helpers.PatternTester;
import org.tudelft.flink.streaming.statemachines.helpers.SymbolConfig;

public class KafkaStateMachines {

    /**
     * Whether NetFlows will be generated using a defined generation function (or be read from file).
     */
    protected static final boolean GENERATE_FLOWS = true;

    /**
     * Whether the arguments of the NetFlow will be ignored and instead the pattern defined in PatternTester will be used.
     */
    protected static final boolean USE_PATTERN_TESTER = true;

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
        FlinkKafkaConsumer010<StateMachineNetFlow> kafkaConsumer = new FlinkKafkaConsumer010<>(
                parameterTool.getRequired("topic"),
                new StateMachineNetFlowSchema(),
                parameterTool.getProperties());

        // create stream
        DataStream<StateMachineNetFlow> netFlowStream = env.addSource(kafkaConsumer);

        // write Kafka stream to standard out.
        DataStream<StateMachineNetFlow> hostSequences = netFlowStream
                .keyBy("IPPair")
                .timeWindow(Time.seconds(10))
                .reduce(new ReduceFunction<StateMachineNetFlow>() {
                    @Override
                    public StateMachineNetFlow reduce(StateMachineNetFlow rollingCount, StateMachineNetFlow newNetFlow) {
                        rollingCount.processFlow(newNetFlow);
                        return rollingCount;
                    }
                });

        // output the results (with a single thread, rather than in parallel)
        hostSequences.print().setParallelism(1);

        if (GENERATE_FLOWS) {
            KafkaProducer producer = new KafkaProducer(env, args);
            if (USE_PATTERN_TESTER) {
                producer.addSourceFunction(getStaticSourceFunction());
            } else {
                producer.addSourceFunction(getFileSourceFunction());
            }
        }

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

    public static SourceFunction<String> getStaticSourceFunction() {
        return new SourceFunction<String>() {
            private static final long serialVersionUID = 6369260225318862378L;
            public boolean running = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                PatternTester tester = new PatternTester();

                while (this.running) {
                    Symbol next = tester.getNext();
                    String data = "{\"AgentID\":\"127.0.0.1\",\"Header\":{\"Version\":9,\"Count\":2,\"SysUpTime\":0,\"UNIXSecs\":1521118700,\"SeqNum\":1602,\"SrcID\":0},\"DataSets\":[[{\"I\":0,\"V\":\"" + next.toString() + "\"},{\"I\":8,\"V\":\"10.0.0.2\"},{\"I\":12,\"V\":\"10.0.0.3\"},{\"I\":15,\"V\":\"0.0.0.0\"},{\"I\":10,\"V\":3},{\"I\":14,\"V\":5},{\"I\":2,\"V\":\"0x000000f5\"},{\"I\":1,\"V\":\"0x000000b6\"},{\"I\":7,\"V\":4242},{\"I\":11,\"V\":80},{\"I\":6,\"V\":\"0x00\"},{\"I\":4,\"V\":17},{\"I\":5,\"V\":1},{\"I\":17,\"V\":\"0x0003\"},{\"I\":16,\"V\":\"0x0002\"},{\"I\":9,\"V\":32},{\"I\":13,\"V\":31},{\"I\":21,\"V\":40536924},{\"I\":22,\"V\":40476924}]]}";
                    ctx.collect(data);
                    Thread.sleep(1);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        };
    }

    public static SourceFunction<String> getFileSourceFunction() {
        return new SourceFunction<String>() {
            private static final long serialVersionUID = 6369260225318862378L;
            public boolean running = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                NetFlowReader reader = new NetFlowReader("input\\WannaCry.uninetflow", NetFlowReader.Format.STRATOSPHERE);
                //NetFlowReader reader = new NetFlowReader("input\\internet-traffic_tshark.txt", NetFlowReader.Format.TSHARK);
                //NetFlowReader reader = new NetFlowReader("input\\15min-skype-call_tshark.txt", NetFlowReader.Format.TSHARK);

                while (reader.hasNext() && this.running) {
                    String flow = reader.getNextJSONFlow();
                    //System.out.println(flow);

                    if (flow != null) {
                        ctx.collect(flow);
                    }
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        };
    }


}