package org.tudelft.flink.streaming.frequentsequences;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class FrequentSequenceNetFlowSchema implements DeserializationSchema<FrequentSequenceNetFlow>, SerializationSchema<FrequentSequenceNetFlow> {

    @Override
    public byte[] serialize(FrequentSequenceNetFlow element) {
        return element.toString().getBytes();
    }

    @Override
    public FrequentSequenceNetFlow deserialize(byte[] message) {
        FrequentSequenceNetFlow flow = new FrequentSequenceNetFlow();
        flow.setFromString(new String(message));
        return flow;
    }

    @Override
    public boolean isEndOfStream(FrequentSequenceNetFlow nextElement) {
        return false;
    }

    @Override
    public TypeInformation<FrequentSequenceNetFlow> getProducedType() {
        return TypeExtractor.getForClass(FrequentSequenceNetFlow.class);
    }
}
