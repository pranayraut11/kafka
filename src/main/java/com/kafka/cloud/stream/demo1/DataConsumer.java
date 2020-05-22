package com.kafka.cloud.stream.demo1;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.cloud.utility.AppConstants;
import com.kafka.cloud.utility.AppSerdis;
import com.kafka.cloud.vo.User;

public class DataConsumer {

	private static final Logger LOG = LoggerFactory.getLogger(DataConsumer.class.getClass());

	Properties prop;

	DataConsumer() {
		prop = new Properties();
		prop.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConstants.STREAM_APPLICATION_ID);
		prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.SERVER_URL);
		prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
	}

	public void consume() {

		StreamsBuilder builder = new StreamsBuilder();

		KStream<Integer, User> K0 = builder.stream(AppConstants.TOPIC_WITH_PARTITION,Consumed.with(AppSerdis.Integer(), AppSerdis.User()));
		K0.foreach((k, v) -> {
			LOG.info("Value {}", v);
		});
		KafkaStreams streams = new KafkaStreams(builder.build(), prop);
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			streams.close();
		}));

	}

	public void consume(int partitionNumber, int offSet) {
		Collection<TopicPartition> patr = new ArrayList<>();
		TopicPartition partition = new TopicPartition(AppConstants.TOPIC_WITH_PARTITION, partitionNumber);
		patr.add(partition);
	}

	public void consume(int partitionNumber) {
		Collection<TopicPartition> patr = new ArrayList<>();
		TopicPartition partition = new TopicPartition(AppConstants.TOPIC_WITH_PARTITION, partitionNumber);
		patr.add(partition);
	}

}
