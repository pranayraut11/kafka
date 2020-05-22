package com.kafka.cloud.demo3;

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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.cloud.utility.AppConstants;
import com.kafka.cloud.utility.AppSerdis;
import com.kafka.cloud.utility.JsonDeserializer;
import com.kafka.cloud.vo.User;

public class DataConsumer {

	private static final Logger LOG = LoggerFactory.getLogger(DataConsumer.class.getClass());

	private KafkaConsumer<Integer, User> consumer;

	DataConsumer() {
		Properties prop = new Properties();
		prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.SERVER_URL);
		prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
		prop.put(ConsumerConfig.GROUP_ID_CONFIG, AppConstants.GROUP_ID);
		prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AppConstants.EARLIEST);
		prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		prop.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, User.class);
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
			ConsumerRecords<Integer, User> records = consumer.poll(Duration.ofMillis(1000));
			records.forEach(record -> {
				LOG.info("Value : {} Partition {} ", record.value(), record.partition());
			});
			consumer.commitAsync();
		}

	}

}
