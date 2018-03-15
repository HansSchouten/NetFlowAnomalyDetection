package org.tudelft.flink.streaming;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class NetFlowSchema implements DeserializationSchema<NetFlow>, SerializationSchema<NetFlow>{

    @Override
    public byte[] serialize(NetFlow element) {
        return element.toString().getBytes();
    }

    @Override
    public NetFlow deserialize(byte[] message) {
        NetFlow flow = new NetFlow();
        flow.setFromString(new String(message));
        return flow;
    }

    @Override
    public boolean isEndOfStream(NetFlow nextElement) {
        return false;
    }

    @Override
    public TypeInformation<NetFlow> getProducedType() {
        return TypeExtractor.getForClass(NetFlow.class);
    }

}
