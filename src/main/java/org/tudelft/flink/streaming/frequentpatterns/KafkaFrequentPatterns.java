package org.tudelft.flink.streaming.frequentpatterns;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

public class KafkaFrequentPatterns {

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
		FlinkKafkaConsumer010<FrequentPatternNetFlow> kafkaConsumer = new FlinkKafkaConsumer010<>(
				parameterTool.getRequired("topic"),
				new FrequentPatternNetFlowSchema(),
				parameterTool.getProperties());

		// create stream
		DataStream<FrequentPatternNetFlow> netflowStream = env.addSource(kafkaConsumer);

		// write Kafka stream to standard out.
		DataStream<FrequentPatternNetFlow> hostPatterns = netflowStream
				.flatMap(new FlatMapFunction<FrequentPatternNetFlow, FrequentPatternNetFlow>() {
					@Override
					public void flatMap(FrequentPatternNetFlow in, Collector<FrequentPatternNetFlow> out) {
						for (FrequentPatternNetFlow flow : in.dataset) {
							out.collect(flow);
						}
					}
				})
				.keyBy("srcIP")
				.timeWindow(Time.seconds(5))
				.reduce(new ReduceFunction<FrequentPatternNetFlow>() {
					@Override
					public FrequentPatternNetFlow reduce(FrequentPatternNetFlow rollingCount, FrequentPatternNetFlow newNetflow) {
						rollingCount.addFlow(newNetflow);
						return rollingCount;
					}
				});

		DataStream<FrequentPatternNetFlow> topN = hostPatterns
				.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
				.reduce(new ReduceFunction<FrequentPatternNetFlow>() {
					@Override
					public FrequentPatternNetFlow reduce(FrequentPatternNetFlow rollingTopN, FrequentPatternNetFlow newHostTopN) throws Exception {
						rollingTopN.mergeTopN(newHostTopN);
						return rollingTopN;
					}
				});

		// output the results (with a single thread, rather than in parallel)
		topN.print();//.setParallelism(1);

		// trigger execution
		env.execute("Kafka NetFlow FrequentPatterns");
	}
}