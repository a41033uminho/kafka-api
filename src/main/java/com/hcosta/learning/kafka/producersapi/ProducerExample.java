package com.hcosta.learning.kafka.producersapi;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerExample {
	
	public static void main(String[] args) {
		 Properties props = new Properties();
	     props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
		 props.put(ProducerConfig.ACKS_CONFIG, "all");
		 props.put(ProducerConfig.RETRIES_CONFIG, 0);
		 props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		 props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		 props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		 props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		 props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		 Producer<String, String> producer = new KafkaProducer<>(props);
		 for (int i = 0; i < 100; i++)
		     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));

		 producer.close();
		 
		 Properties propsBatch = new Properties();
		 propsBatch.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
		 propsBatch.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");
		 propsBatch.put(ProducerConfig.ACKS_CONFIG, "all");
		 propsBatch.put(ProducerConfig.RETRIES_CONFIG, 2);
		 propsBatch.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		 propsBatch.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		 propsBatch.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		 
		 Producer<String, String> producerBatch = new KafkaProducer<>(propsBatch, new StringSerializer(), new StringSerializer());
		 producerBatch.initTransactions();

		 try {
			 producerBatch.beginTransaction();
		     for (int i = 0; i < 100; i++)
		    	 producerBatch.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
		     producerBatch.commitTransaction();
		 } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
		     // We can't recover from these exceptions, so our only option is to close the producer and exit.
			 producerBatch.close();
		 } catch (KafkaException e) {
		     // For all other exceptions, just abort the transaction and try again.
			 producerBatch.abortTransaction();
		 }
		 producerBatch.close();
	}

}
