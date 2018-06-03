package org.tudelft.flink.streaming;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.tudelft.flink.streaming.statemachines.Symbol;
import org.tudelft.flink.streaming.statemachines.helpers.PatternTester;

public class OldKafkaProducer {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if(parameterTool.getNumberOfParameters() < 2) {
            System.out.println("Missing parameters!");
            System.out.println("Usage: Kafka --topic <topic> --bootstrap.servers <kafka brokers>");
            return;
        }

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));

        // DEBUG FUNCTION
        //DataStream<String> messageStream = env.addSource(getDebugSourceFunction());
        // PATTERN TESTER FUNCTION
        DataStream<String> messageStream = env.addSource(getStaticSourceFunction());
        // FILE FUNCTION
        //DataStream<String> messageStream = env.addSource(getFileSourceFunction());

        FlinkKafkaProducer010<String> producer = new FlinkKafkaProducer010<>(parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties());
        producer.setFlushOnCheckpoint(true);
        messageStream.addSink(producer);
        env.execute("Produce data on Kafka");
    }


    public static SourceFunction<String> getDebugSourceFunction() {
        return new SourceFunction<String>() {
            private static final long serialVersionUID = 6369260225318862378L;
            public boolean running = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int counter = 0;
                while (this.running && counter < 500) {
                    counter++;

                    String data = "item " + Integer.toString(counter);
                    ctx.collect(data);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        };
    }


    public static SourceFunction<String> getStaticSourceFunction() {
        return new SourceFunction<String>() {
            private static final long serialVersionUID = 6369260225318862378L;
            public boolean running = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                PatternTester tester = new PatternTester();
                int counter = 0;
                while (this.running) {
                    // LIMIT
                    if (counter == 500) {
                        break;
                    }
                    counter++;

                    Symbol next = tester.getNext();
                    String data = "{\"AgentID\":\"127.0.0.1\",\"Header\":{\"Version\":9,\"Count\":2,\"SysUpTime\":0,\"UNIXSecs\":1521118700,\"SeqNum\":1602,\"SrcID\":0},\"DataSets\":[[{\"I\":0,\"V\":\"" + next.toString() + "\"},{\"I\":8,\"V\":\"10.0.0.2\"},{\"I\":12,\"V\":\"10.0.0.3\"},{\"I\":15,\"V\":\"0.0.0.0\"},{\"I\":10,\"V\":3},{\"I\":14,\"V\":5},{\"I\":2,\"V\":\"0x000000f5\"},{\"I\":1,\"V\":\"0x000000b6\"},{\"I\":7,\"V\":4242},{\"I\":11,\"V\":80},{\"I\":6,\"V\":\"0x00\"},{\"I\":4,\"V\":17},{\"I\":5,\"V\":1},{\"I\":17,\"V\":\"0x0003\"},{\"I\":16,\"V\":\"0x0002\"},{\"I\":9,\"V\":32},{\"I\":13,\"V\":31},{\"I\":21,\"V\":40536924},{\"I\":22,\"V\":40476924}]]}";
                    ctx.collect(data);

                    // DEBUG
                    System.out.println("send: " + next.toString());
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
