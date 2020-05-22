package com.kafka.cloud.demo1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.cloud.utility.AppConstants;

public class DataConsumer {

	private static final Logger LOG = LoggerFactory.getLogger(DataConsumer.class.getClass());

	private KafkaConsumer<Integer, String> consumer;

	DataConsumer() {
		Properties prop = new Properties();
		prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.SERVER_URL);
		prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
		prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, AppConstants.GROUP_ID);
		prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AppConstants.EARLIEST);
		consumer = new KafkaConsumer<>(prop);
		consumer.subscribe(Collections.singleton(AppConstants.TOPIC));
	}

	public void consume() {
		while (true) {
			ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(1000));
			records.forEach(record -> {
				LOG.info("Value : {}", record.value());
			});
			consumer.commitAsync();
		}
	}

}
