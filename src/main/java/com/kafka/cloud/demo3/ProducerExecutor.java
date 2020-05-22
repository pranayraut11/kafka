package com.kafka.cloud.demo3;

import java.util.concurrent.atomic.AtomicInteger;

import com.kafka.cloud.utility.DataGenerator;

public class ProducerExecutor {
	public static void main(String[] args) {

		DataProducer producer = new DataProducer();

		AtomicInteger counter = new AtomicInteger();

		DataGenerator.generateUserData().forEach(user -> {
			producer.send(producer.generateData(counter.get(), user));
			counter.incrementAndGet();
		});

		/*
		 * for (int i = 0; i < AppConstants.DATA_GENERATOR_SIZE; i++) {
		 * producer.send(producer.generateData(i, " Demo Message in partition 0 " + i,
		 * 0)); producer.send(producer.generateData(i, " Demo Message in partition 1" +
		 * i, 1)); producer.send(producer.generateData(i, " Demo Message in partition 2"
		 * + i, 2)); }
		 */
		producer.close();

	}
}
