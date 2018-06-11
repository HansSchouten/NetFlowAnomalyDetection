package org.tudelft.flink.streaming;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.tudelft.flink.streaming.statemachines.Symbol;
import org.tudelft.flink.streaming.statemachines.helpers.PatternTester;

import java.util.Properties;

public class KafkaProducer {

    public static String topic = "vflow.netflow9";

    public static void produce() throws Exception {
        // Set properties used to configure the producer
        Properties properties = new Properties();
        // Set the brokers (bootstrap servers)
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // Set how to serialize key/value pairs
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);

        //produceDebug(producer);
        producePatterns(producer);
    }

    protected static void produceDebug(org.apache.kafka.clients.producer.KafkaProducer<String, String> producer) throws Exception {
        String progressAnimation = "|/-\\";
        for(int i = 0; i < 5000; i++) {
            // Pick a sentence at random
            String data = "Item " + Integer.toString(i);
            // Send the sentence to the test topic
            producer.send(new ProducerRecord<String, String>(topic, data));

            String progressBar = "\r" + progressAnimation.charAt(i % progressAnimation.length()) + " " + i;
            System.out.write(progressBar.getBytes());
        }
    }

    protected static void producePatterns(org.apache.kafka.clients.producer.KafkaProducer<String, String> producer) throws Exception {
        PatternTester tester = new PatternTester();
        int counter = 0;
        while (counter < 10000000) {
            counter++;

            Symbol next = tester.getNext();
            String data = "{\"AgentID\":\"127.0.0.1\",\"Header\":{\"Version\":9,\"Count\":2,\"SysUpTime\":0,\"UNIXSecs\":1521118700,\"SeqNum\":1602,\"SrcID\":0},\"DataSets\":[[{\"I\":0,\"V\":\"" + next.toString() + "\"},{\"I\":8,\"V\":\"10.0.0.2\"},{\"I\":12,\"V\":\"10.0.0.3\"},{\"I\":15,\"V\":\"0.0.0.0\"},{\"I\":10,\"V\":3},{\"I\":14,\"V\":5},{\"I\":2,\"V\":\"0x000000f5\"},{\"I\":1,\"V\":\"0x000000b6\"},{\"I\":7,\"V\":4242},{\"I\":11,\"V\":80},{\"I\":6,\"V\":\"0x00\"},{\"I\":4,\"V\":17},{\"I\":5,\"V\":1},{\"I\":17,\"V\":\"0x0003\"},{\"I\":16,\"V\":\"0x0002\"},{\"I\":9,\"V\":32},{\"I\":13,\"V\":31},{\"I\":21,\"V\":40536924},{\"I\":22,\"V\":40476924}]]}";
            producer.send(new ProducerRecord<String, String>(topic, data));

            //Thread.sleep(500);
        }
    }

    public static void main(String[] args) throws Exception {
        produce();
    }
}