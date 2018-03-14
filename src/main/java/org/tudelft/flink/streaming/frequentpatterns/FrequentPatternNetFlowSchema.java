package org.tudelft.flink.streaming.frequentpatterns;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.tudelft.flink.streaming.RandomWeightedCollection;

public class FrequentPatternNetFlowSchema implements DeserializationSchema<FrequentPatternNetFlow>, SerializationSchema<FrequentPatternNetFlow> {

    protected RandomWeightedCollection srcIPs = new RandomWeightedCollection();
    protected RandomWeightedCollection dstIPs = new RandomWeightedCollection();
    protected RandomWeightedCollection srcPorts = new RandomWeightedCollection();
    protected RandomWeightedCollection dstPorts = new RandomWeightedCollection();

    public FrequentPatternNetFlowSchema(){
        srcIPs.add(0.5, "10.0.0.0");
        srcIPs.add(0.25, "10.0.0.1");
        srcIPs.add(0.25, "10.0.0.2");

        dstIPs.add(0.5, "10.0.1.1");
        dstIPs.add(0.5, "10.0.1.2");

        srcPorts.add(0.5, "80");
        srcPorts.add(0.5, "443");

        dstPorts.add(1.0, "8080");
    }

    public void setRandomValues(FrequentPatternNetFlow netflow) {
        netflow.srcIP = srcIPs.randomEntry();
        netflow.dstIP = dstIPs.randomEntry();
        netflow.srcPort = srcPorts.randomEntry();
        netflow.dstPort = dstPorts.randomEntry();
    }

    @Override
    public byte[] serialize(FrequentPatternNetFlow element) {
        return element.toString().getBytes();
    }

    @Override
    public FrequentPatternNetFlow deserialize(byte[] message) {
        FrequentPatternNetFlow flow = new FrequentPatternNetFlow();
        flow.setFromString(new String(message));
        this.setRandomValues(flow);     // RANDOM VALUES
        return flow;
    }

    @Override
    public boolean isEndOfStream(FrequentPatternNetFlow nextElement) {
        return false;
    }

    @Override
    public TypeInformation<FrequentPatternNetFlow> getProducedType() {
        return TypeExtractor.getForClass(FrequentPatternNetFlow.class);
    }
}
