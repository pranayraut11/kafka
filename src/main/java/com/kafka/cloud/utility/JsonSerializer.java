package com.kafka.cloud.utility;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerializer<T> implements Serializer<T> {

	private final ObjectMapper mapper = new ObjectMapper();

	@Override
	public byte[] serialize(String topic, T data) {

		if (data == null) {
			return null;
		}
		try {
			return mapper.writeValueAsBytes(data);
		} catch (JsonProcessingException e) {
			throw new SerializationException("Error Serializing json messgae ", e);
		}

	}

}
