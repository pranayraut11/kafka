package com.kafka.cloud.utility;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.support.serializer.DeserializationException;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonDeserializer<T> implements Deserializer<T> {

	private final ObjectMapper mapper = new ObjectMapper();

	private Class<T> className;
	public static final String KEY_CLASS_NAME_CONFIG = "key.class.name";
	public static final String VALUE_CLASS_NAME_CONFIG = "value.class.name";
	boolean isKey;

	@SuppressWarnings("unchecked")
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.isKey = isKey;
		if (isKey)
			className = (Class<T>) configs.get(KEY_CLASS_NAME_CONFIG);
		else
			className = (Class<T>) configs.get(VALUE_CLASS_NAME_CONFIG);
		// TODO Auto-generated method stub
	}

	@Override
	public T deserialize(String topic, byte[] data) {

		if (data == null) {
			return null;
		}
		try {
			return mapper.readValue(data, className);
		} catch (IOException e) {
			throw new DeserializationException("Error while deserialzing ", data, isKey, e);
		}
	}

}
