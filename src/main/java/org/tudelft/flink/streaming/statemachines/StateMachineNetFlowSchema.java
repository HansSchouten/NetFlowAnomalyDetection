package org.tudelft.flink.streaming.statemachines;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class StateMachineNetFlowSchema implements DeserializationSchema<StateMachineNetFlow>, SerializationSchema<StateMachineNetFlow> {

    @Override
    public byte[] serialize(StateMachineNetFlow element) {
        return element.toString().getBytes();
    }

    @Override
    public StateMachineNetFlow deserialize(byte[] message) {
        StateMachineNetFlow flow = new StateMachineNetFlow();
        flow.setFromString(new String(message));
        return flow;
    }

    @Override
    public boolean isEndOfStream(StateMachineNetFlow nextElement) {
        return false;
    }

    @Override
    public TypeInformation<StateMachineNetFlow> getProducedType() {
        return TypeExtractor.getForClass(StateMachineNetFlow.class);
    }
}
