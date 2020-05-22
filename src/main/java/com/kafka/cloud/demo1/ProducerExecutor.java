package com.kafka.cloud.demo1;

import com.kafka.cloud.utility.AppConstants;

public class ProducerExecutor {
	public static void main(String[] args) {

		DataProducer producer = new DataProducer();
		for (int i = 0; i < AppConstants.DATA_GENERATOR_SIZE; i++) {
			producer.generateData(null, " Demo Message " + i);
		}
		producer.close();

	}
}
