package com.hcosta.learning.kafka.consumerapi;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerExample {
	public static void main(String[] args) {
	     Properties props = new Properties();
	     props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
	     props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
	     props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
	     props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
	     props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	     props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	     try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
			consumer.subscribe(Arrays.asList("my-topic", "bar"));
			 while (true) {
			     ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			     for (ConsumerRecord<String, String> record : records)
			         System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
			 }
		}
	}

}
