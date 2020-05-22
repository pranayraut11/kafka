package com.kafka.cloud.demo2;

import com.kafka.cloud.utility.AppConstants;

public class ProducerExecutor {
	public static void main(String[] args) {

		DataProducer producer = new DataProducer();
		for (int i = 0; i < AppConstants.DATA_GENERATOR_SIZE; i++) {
			producer.send(producer.generateData(i, " Demo Message " + i));
		}
		for (int i = 0; i < AppConstants.DATA_GENERATOR_SIZE; i++) {
			producer.send(producer.generateData(i, " Demo Message in partition 0 " + i, 0));
			producer.send(producer.generateData(i, " Demo Message in partition 1" + i, 1));
			producer.send(producer.generateData(i, " Demo Message in partition 2" + i, 2));
		}
		producer.close();

	}
}
