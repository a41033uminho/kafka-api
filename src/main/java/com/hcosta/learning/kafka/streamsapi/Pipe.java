package com.hcosta.learning.kafka.streamsapi;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

public class Pipe {

	public static void main(String[] args) {

		final Properties props = new Properties();
		// Pipe output save ? 
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
		// Default Key and Value "Readers"
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		// computational logic of our Streams application
		// Kafka Streams this computational logic is defined as a topology of connected processor nodes. We can use a topology builder to construct such a topology,
		final StreamsBuilder builder = new StreamsBuilder();

		// create a source stream from a Kafka topic named streams-plaintext-input using this topology builder
		KStream<String, String> source = builder.stream("streams-plaintext-input");

		// The simplest thing we can do with this stream is to write it into another Kafka topic, say it's named streams-pipe-output: 
		source.to("streams-pipe-output");

		// We can inspect what kind of topology is created from this builder by doing the following
		final Topology topology = builder.build();
		System.out.println(topology.describe());

		final KafkaStreams streams = new KafkaStreams(topology, props);
		
		final CountDownLatch latch = new CountDownLatch(1);

		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});
		
		try {
		    streams.start();
		    latch.await();
		} catch (Throwable e) {
		    System.exit(1);
		}
		System.exit(0);


	}

}
