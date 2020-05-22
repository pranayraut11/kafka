package com.kafka.cloud.demo2;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.cloud.utility.AppConstants;

public class DataProducer {
	public static final Logger LOG = LoggerFactory.getLogger(DataProducer.class.getClass());
	private Producer<Integer, String> producer = null;

	DataProducer() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.SERVER_URL);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producer = new KafkaProducer<>(properties);
	}

	public ProducerRecord<Integer, String> generateData(Integer key, String value) {
		LOG.info("-----Sending data------");
		LOG.info("-----Key : {} Value : {}", key, value);
		return new ProducerRecord<Integer, String>(AppConstants.TOPIC_WITH_PARTITION, key, value);
	}

	public ProducerRecord<Integer, String> generateData(Integer key, String value, int partitionNumber) {
		LOG.info("-----Sending data------");
		LOG.info("-----Key : {} Value : {}", key, value);
		return new ProducerRecord<Integer, String>(AppConstants.TOPIC_WITH_PARTITION, partitionNumber, key, value);
	}

	public void send(ProducerRecord<Integer, String> record) {
		producer.send(record, new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				LOG.debug("Partition {} : Offset is {}", metadata.partition(), metadata.offset());
			}
		});
	}

	public void close() {
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			producer.close();
		}));

	}

}
