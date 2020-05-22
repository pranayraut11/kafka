package com.kafka.cloud.serdis;

public class ConsumerExecuter {

	public static void main(String[] args) {
		DataConsumer consumer = new DataConsumer();
		consumer.consume(1,0);
	}

}
