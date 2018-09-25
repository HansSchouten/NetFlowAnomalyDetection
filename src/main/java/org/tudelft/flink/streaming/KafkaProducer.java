package org.tudelft.flink.streaming;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.tudelft.flink.streaming.statemachines.StateMachineNetFlow;
import org.tudelft.flink.streaming.statemachines.Symbol;
import org.tudelft.flink.streaming.statemachines.helpers.PatternTester;

import java.util.ArrayList;
import java.util.List;
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

        producePerformanceTestData(producer);
        //produceDebug(producer);
        //producePatterns(producer);
        //replayStratosphere(producer);
    }

    protected static void producePerformanceTestData(org.apache.kafka.clients.producer.KafkaProducer<String, String> producer) throws Exception {
        /*
        List<PatternTester> testers = new ArrayList<>();
        for (int i=3; i<4; i++) {
            String path = "input\\pautomac\\validation\\set" + (i+1) + "\\0-1pautomac.train";
            testers.add(new PatternTester(path));
        }

        int max = 1000000;
        for (int count = 0; count < max; count++) {
            for (int i=0; i<1; i++) {
                PatternTester tester = testers.get(i);
                Symbol next = tester.getNext();
                String last = (count > 0.95 * max) ? "1" : "0";
                String data = "{\"AgentID\":\"127.0.0.1\",\"DataSets\":[[{\"I\":0,\"V\":\"" + next.toString() + "\"},{\"I\":-1,\"V\":\"" + (i+1) + "\"},{\"I\":-2,\"V\":\"" + last + "\"},{\"I\":8,\"V\":\"10.0.0.2\"},{\"I\":12,\"V\":\"10.0.0.3\"},{\"I\":15,\"V\":\"0.0.0.0\"},{\"I\":10,\"V\":3},{\"I\":14,\"V\":5},{\"I\":2,\"V\":\"0x000000f5\"},{\"I\":1,\"V\":\"0x000000b6\"},{\"I\":7,\"V\":4242},{\"I\":11,\"V\":80},{\"I\":6,\"V\":\"0x00\"},{\"I\":4,\"V\":17},{\"I\":5,\"V\":1},{\"I\":17,\"V\":\"0x0003\"},{\"I\":16,\"V\":\"0x0002\"},{\"I\":9,\"V\":32},{\"I\":13,\"V\":31},{\"I\":21,\"V\":40536924},{\"I\":22,\"V\":40476924}]]}";
                producer.send(new ProducerRecord<String, String>(topic, data));
            }
            if (count % 100 == 0) {
                Thread.sleep(1);
            }
        }
        */


        int dataset = 6;
        String path = "input\\pautomac\\validation\\set" + dataset + "\\0-1pautomac.train";
        PatternTester tester = new PatternTester(path);
        int max = 1000000;
        for (int count = 0; count < max; count++) {
            Symbol next = tester.getNext();
            String last = (count > 0.95 * max) ? "1" : "0";
            String data = "{\"AgentID\":\"127.0.0.1\",\"DataSets\":[[{\"I\":0,\"V\":\"" + next.toString() + "\"},{\"I\":-1,\"V\":\"" + dataset + "\"},{\"I\":-2,\"V\":\"" + last + "\"},{\"I\":8,\"V\":\"10.0.0.2\"},{\"I\":12,\"V\":\"10.0.0.3\"},{\"I\":15,\"V\":\"0.0.0.0\"},{\"I\":10,\"V\":3},{\"I\":14,\"V\":5},{\"I\":2,\"V\":\"0x000000f5\"},{\"I\":1,\"V\":\"0x000000b6\"},{\"I\":7,\"V\":4242},{\"I\":11,\"V\":80},{\"I\":6,\"V\":\"0x00\"},{\"I\":4,\"V\":17},{\"I\":5,\"V\":1},{\"I\":17,\"V\":\"0x0003\"},{\"I\":16,\"V\":\"0x0002\"},{\"I\":9,\"V\":32},{\"I\":13,\"V\":31},{\"I\":21,\"V\":40536924},{\"I\":22,\"V\":40476924}]]}";
            producer.send(new ProducerRecord<String, String>(topic, data));
            if (count % 100 == 0) {
                Thread.sleep(1);
            }
        }


        /*
        int dataset = 2;
        int max = 500000;
        List<String> c = new ArrayList<>();

        List<String> c1 = new ArrayList<>();
        c1.add("1000");
        c1.add("750");
        c1.add("500");
        c1.add("250");
        c1.add("100");
        c1.add("50");
        c1.add("25");
        c1.add("10");

        List<String> c2 = new ArrayList<>();
        c2.add("50");
        c2.add("25");
        c2.add("10");
        c2.add("5");

        for (String c1_el : c1) {
            for (String c2_el : c2) {
                c.add(c1_el + "-" + c2_el);
            }
        }

        String path = "input\\pautomac\\design\\set" + dataset + "\\0-1pautomac.train";
        PatternTester tester = new PatternTester(path);
        for (String combination : c) {
            tester.reset();
            for (int count = 0; count < max; count++) {
                Symbol next = tester.getNext();
                String last = (count > 0.9 * max) ? "1" : "0";
                String data = "{\"AgentID\":\"127.0.0.1\",\"DataSets\":[[{\"I\":0,\"V\":\"" + next.toString() + "\"},{\"I\":-1,\"V\":\"" + dataset + "\"},{\"I\":-2,\"V\":\"" + last + "\"},{\"I\":-3,\"V\":\"" + combination + "\"},{\"I\":8,\"V\":\"10.0.0.2\"},{\"I\":12,\"V\":\"10.0.0.3\"},{\"I\":15,\"V\":\"0.0.0.0\"},{\"I\":10,\"V\":3},{\"I\":14,\"V\":5},{\"I\":2,\"V\":\"0x000000f5\"},{\"I\":1,\"V\":\"0x000000b6\"},{\"I\":7,\"V\":4242},{\"I\":11,\"V\":80},{\"I\":6,\"V\":\"0x00\"},{\"I\":4,\"V\":17},{\"I\":5,\"V\":1},{\"I\":17,\"V\":\"0x0003\"},{\"I\":16,\"V\":\"0x0002\"},{\"I\":9,\"V\":32},{\"I\":13,\"V\":31},{\"I\":21,\"V\":40536924},{\"I\":22,\"V\":40476924}]]}";
                producer.send(new ProducerRecord<String, String>(topic, data));
                if (count % 100 == 0) {
                    Thread.sleep(1);
                }
            }
        }
        */
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
        while (counter < 10000) {
            counter++;

            Symbol next = tester.getNext();
            //System.out.println(counter + " : " + next.toString());
            String data = "{\"AgentID\":\"127.0.0.1\",\"Header\":{\"Version\":9,\"Count\":2,\"SysUpTime\":0,\"UNIXSecs\":1521118700,\"SeqNum\":1602,\"SrcID\":0},\"DataSets\":[[{\"I\":0,\"V\":\"" + next.toString() + "\"},{\"I\":8,\"V\":\"10.0.0.2\"},{\"I\":12,\"V\":\"10.0.0.3\"},{\"I\":15,\"V\":\"0.0.0.0\"},{\"I\":10,\"V\":3},{\"I\":14,\"V\":5},{\"I\":2,\"V\":\"0x000000f5\"},{\"I\":1,\"V\":\"0x000000b6\"},{\"I\":7,\"V\":4242},{\"I\":11,\"V\":80},{\"I\":6,\"V\":\"0x00\"},{\"I\":4,\"V\":17},{\"I\":5,\"V\":1},{\"I\":17,\"V\":\"0x0003\"},{\"I\":16,\"V\":\"0x0002\"},{\"I\":9,\"V\":32},{\"I\":13,\"V\":31},{\"I\":21,\"V\":40536924},{\"I\":22,\"V\":40476924}]]}";
            producer.send(new ProducerRecord<String, String>(topic, data));
        }
    }

    protected static void replayStratosphere(org.apache.kafka.clients.producer.KafkaProducer<String, String> producer) throws Exception {
        //NetFlowReader reader = new NetFlowReader("input\\cryptowall.uninetflow", NetFlowReader.Format.STRATOSPHERE);
        //NetFlowReader reader = new NetFlowReader("input\\WannaCry.uninetflow", NetFlowReader.Format.STRATOSPHERE);
        //NetFlowReader reader = new NetFlowReader("input\\artemis_311.uninetflow", NetFlowReader.Format.STRATOSPHERE);
        //NetFlowReader reader = new NetFlowReader("input\\0200.txt", NetFlowReader.Format.NFDUMP);
        //NetFlowReader reader = new NetFlowReader("input\\internet-traffic_tshark.txt", NetFlowReader.Format.TSHARK);
        //NetFlowReader reader = new NetFlowReader("input\\15min-skype-call_tshark.txt", NetFlowReader.Format.TSHARK);

        // Learn from samples
        //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\malware\\sample1 botnet131\\131-1.binetflow", NetFlowReader.Format.STRATOSPHERE);
        //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\malware\\sample2 botnet134\\134-1.binetflow", NetFlowReader.Format.STRATOSPHERE);
        // no StateMachines
        //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\malware\\sample3 botnet135\\135-1.binetflow", NetFlowReader.Format.STRATOSPHERE);
        // sample 4
        //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\malware\\sample4 botnet221\\221-1.binetflow", NetFlowReader.Format.STRATOSPHERE);

        // learn emotet (only ARP fingerprints, which is a protocol not included in Eduroam traffic)
        //NetFlowReader reader = new NetFlowReader("input\\emotet\\botnet-264-1.txt", NetFlowReader.Format.CUSTOM_NFDUMP);
        //NetFlowReader reader = new NetFlowReader("input\\emotet\\botnet-264-2.txt", NetFlowReader.Format.CUSTOM_NFDUMP);
        //NetFlowReader reader = new NetFlowReader("input\\emotet\\botnet-276-1.txt", NetFlowReader.Format.CUSTOM_NFDUMP);
        //NetFlowReader reader = new NetFlowReader("input\\emotet\\botnet-276-2.txt", NetFlowReader.Format.CUSTOM_NFDUMP);
        // NetFlowReader reader = new NetFlowReader("input\\emotet\\botnet-279-1.txt", NetFlowReader.Format.CUSTOM_NFDUMP);

        // learn Kovter
        //NetFlowReader reader = new NetFlowReader("input\\kovter\\2017-06-28.txt", NetFlowReader.Format.CUSTOM_NFDUMP);
        //NetFlowReader reader = new NetFlowReader("input\\kovter\\2017-06-29.txt", NetFlowReader.Format.CUSTOM_NFDUMP);
        //NetFlowReader reader = new NetFlowReader("input\\kovter\\angler.txt", NetFlowReader.Format.CUSTOM_NFDUMP);
        //NetFlowReader reader = new NetFlowReader("input\\kovter\\fiesta.txt", NetFlowReader.Format.CUSTOM_NFDUMP);

        // Detect on mixed
        //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\mixed\\mixed1-botnet131.binetflow", NetFlowReader.Format.STRATOSPHERE);
        //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\mixed\\mixed2-botnet134.binetflow", NetFlowReader.Format.STRATOSPHERE);
        //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\mixed\\mixed4-botnet221.binetflow", NetFlowReader.Format.STRATOSPHERE);

        // learn 5min pcap -> flow samples
            //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\malware\\131-1-Bubble-Dock-(mixed-1).txt", NetFlowReader.Format.CUSTOM_NFDUMP);
        //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\malware\\134-1-Crypto-Wall-3.0-(mixed-2).txt", NetFlowReader.Format.CUSTOM_NFDUMP);
        //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\malware\\135-1-Stlrat-(mixed-3).txt", NetFlowReader.Format.CUSTOM_NFDUMP);
        //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\malware\\214-1-Locky-(mixed-4).txt", NetFlowReader.Format.CUSTOM_NFDUMP);
        //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\malware\\320-1-CCleaner-Trojan.txt", NetFlowReader.Format.CUSTOM_NFDUMP);
            //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\malware\\338-1-CoinMiner.txt", NetFlowReader.Format.CUSTOM_NFDUMP);
        //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\malware\\342-1-Miner-Trojan.txt", NetFlowReader.Format.CUSTOM_NFDUMP);
            //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\malware\\345-1-Cobalt.txt", NetFlowReader.Format.CUSTOM_NFDUMP);
            //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\malware\\346-1-Dridex.txt", NetFlowReader.Format.CUSTOM_NFDUMP);
            //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\malware\\347-1-BitCoinMiner.txt", NetFlowReader.Format.CUSTOM_NFDUMP);
        //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\malware\\348-1-HTBot.txt", NetFlowReader.Format.CUSTOM_NFDUMP);
        //NetFlowReader reader = new NetFlowReader("input\\stratosphere\\malware\\349-1-Adload.txt", NetFlowReader.Format.CUSTOM_NFDUMP);

        NetFlowReader reader = new NetFlowReader("input\\fingerprints\\vertical-scan\\vertical-scan.txt", NetFlowReader.Format.CUSTOM_NFDUMP);

        int counter = 0;
        while (counter >= 0) {

            if (reader.hasNext()) {
                String data = reader.getNextJSONFlow();

                if (data != null) {
                    producer.send(new ProducerRecord<String, String>(topic, data));
                }
            } else {
                break;
            }

            counter++;
        }
    }

    public static void main(String[] args) throws Exception {
        produce();
    }
}