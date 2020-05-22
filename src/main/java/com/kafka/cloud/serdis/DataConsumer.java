package com.kafka.cloud.serdis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
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
	}

	public void consume() {
		consumer.subscribe(Collections.singleton(AppConstants.TOPIC_WITH_PARTITION));
		poll();
	}

	public void consume(int partitionNumber, int offSet) {
		Collection<TopicPartition> patr = new ArrayList<>();
		TopicPartition partition = new TopicPartition(AppConstants.TOPIC_WITH_PARTITION, partitionNumber);
		patr.add(partition);
		consumer.assign(patr);
		consumer.seek(partition, offSet);
		poll();
	}

	public void consume(int partitionNumber) {
		Collection<TopicPartition> patr = new ArrayList<>();
		TopicPartition partition = new TopicPartition(AppConstants.TOPIC_WITH_PARTITION, partitionNumber);
		patr.add(partition);
		consumer.assign(patr);
		poll();
	}

	private void poll() {
		while (true) {
			ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(1000));
			records.forEach(record -> {
				LOG.info("Value : {} Partition {} ", record.value(), record.partition());
			});
			consumer.commitAsync();
		}

	}

}
