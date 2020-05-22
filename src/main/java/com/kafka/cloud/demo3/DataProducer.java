package com.kafka.cloud.demo3;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.cloud.utility.AppConstants;
import com.kafka.cloud.utility.AppSerdis;
import com.kafka.cloud.vo.User;

public class DataProducer {
	public static final Logger LOG = LoggerFactory.getLogger(DataProducer.class.getClass());
	private Producer<Integer, User> producer = null;

	DataProducer() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.SERVER_URL);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AppSerdis.User().serializer().getClass().getName());
		producer = new KafkaProducer<>(properties);
	}

	public ProducerRecord<Integer, User> generateData(Integer key, User user) {
		LOG.info("-----Sending data------");
		LOG.info("-----Key : {} Value : {}", key, user);
		return new ProducerRecord<Integer, User>(AppConstants.TOPIC_WITH_PARTITION, key, user);
	}

	public ProducerRecord<Integer, String> generateData(Integer key, String value, int partitionNumber) {
		LOG.info("-----Sending data------");
		LOG.info("-----Key : {} Value : {}", key, value);
		return new ProducerRecord<Integer, String>(AppConstants.TOPIC_WITH_PARTITION, partitionNumber, key, value);
	}

	public void send(ProducerRecord<Integer, User> record) {
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
