package org.tudelft.flink.streaming;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class MetricSchema implements DeserializationSchema<Metric>, SerializationSchema<Metric>{
	
	@Override
	public byte[] serialize(Metric element) {
		return element.toString().getBytes();
	}

	@Override
	public Metric deserialize(byte[] message) {
		return Metric.fromString(new String(message));
	}

	@Override
	public boolean isEndOfStream(Metric nextElement) {
		return false;
	}

	@Override
	public TypeInformation<Metric> getProducedType() {
		return TypeExtractor.getForClass(Metric.class);
	}

}
