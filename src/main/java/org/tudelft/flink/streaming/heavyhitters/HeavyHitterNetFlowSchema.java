package org.tudelft.flink.streaming.heavyhitters;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.tudelft.flink.streaming.RandomWeightedCollection;

public class HeavyHitterNetFlowSchema implements DeserializationSchema<HeavyHitterNetFlow>, SerializationSchema<HeavyHitterNetFlow> {

    protected RandomWeightedCollection srcIPs = new RandomWeightedCollection();
    protected RandomWeightedCollection dstIPs = new RandomWeightedCollection();
    protected RandomWeightedCollection srcPorts = new RandomWeightedCollection();
    protected RandomWeightedCollection dstPorts = new RandomWeightedCollection();

    public HeavyHitterNetFlowSchema(){
        srcIPs.add(0.5, "10.0.0.0");
        srcIPs.add(0.25, "10.0.0.1");
        srcIPs.add(0.25, "10.0.0.2");

        dstIPs.add(0.5, "10.0.1.1");
        dstIPs.add(0.5, "10.0.1.2");

        srcPorts.add(0.5, "80");
        srcPorts.add(0.5, "443");

        dstPorts.add(1.0, "8080");
    }

    public void setRandomValues(HeavyHitterNetFlow netflow) {
        netflow.srcIP = (String) srcIPs.randomEntry();
        netflow.dstIP = (String) dstIPs.randomEntry();
        netflow.srcPort = (Integer) srcPorts.randomEntry();
        netflow.dstPort = (Integer) dstPorts.randomEntry();
    }

    @Override
    public byte[] serialize(HeavyHitterNetFlow element) {
        return element.toString().getBytes();
    }

    @Override
    public HeavyHitterNetFlow deserialize(byte[] message) {
        HeavyHitterNetFlow flow = new HeavyHitterNetFlow();
        flow.setFromString(new String(message));
        this.setRandomValues(flow);     // RANDOM VALUES
        return flow;
    }

    @Override
    public boolean isEndOfStream(HeavyHitterNetFlow nextElement) {
        return false;
    }

    @Override
    public TypeInformation<HeavyHitterNetFlow> getProducedType() {
        return TypeExtractor.getForClass(HeavyHitterNetFlow.class);
    }
}
