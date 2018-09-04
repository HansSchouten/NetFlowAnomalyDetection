package org.tudelft.flink.streaming.statemachines;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.tudelft.flink.streaming.statemachines.helpers.PatternTester;
import org.tudelft.flink.streaming.statemachines.helpers.SymbolConfig;

public class StateMachineNetFlowSchema implements DeserializationSchema<StateMachineNetFlow>, SerializationSchema<StateMachineNetFlow> {

    @Override
    public byte[] serialize(StateMachineNetFlow element) {
        return element.toString().getBytes();
    }

    @Override
    public StateMachineNetFlow deserialize(byte[] message) {
        StateMachineNetFlow flow = new StateMachineNetFlow();
        flow.setFromString(new String(message));

        // TODO: read SymbolConfig from getGlobalJobParameters
        // https://brewing.codes/2017/10/24/flink-additional-data/
        //flow.symbolConfig = new SymbolConfig();

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
