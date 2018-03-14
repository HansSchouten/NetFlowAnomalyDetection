package org.tudelft.flink.streaming;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class NetflowSchema implements DeserializationSchema<Netflow>, SerializationSchema<Netflow>{

    @Override
    public byte[] serialize(Netflow element) {
        return element.toString().getBytes();
    }

    @Override
    public Netflow deserialize(byte[] message) {
        Netflow flow = new Netflow();
        flow.setFromString(new String(message));
        return flow;
    }

    @Override
    public boolean isEndOfStream(Netflow nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Netflow> getProducedType() {
        return TypeExtractor.getForClass(Netflow.class);
    }

}
