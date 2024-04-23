package com.vhugenthobler;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder(); // Instantiating the stream Builder

        KStream<String, String> streamInput = streamsBuilder
                .stream("wordcount-input"); // Create a Stream from Kafka

        KTable<String, Long> wordCount = streamInput
                .mapValues(value ->value.toLowerCase()) // MapValues to lowercase
                .flatMapValues(value -> Arrays.asList(value.split(" "))) // FlatMapValues splitting words by space
                .selectKey((key, value) -> value) // SelectKey to replace it by its value
                .groupByKey() // GroupByKey the streams by key
                .count(Named.as("count")); // Count occurrences in each group

        wordCount.toStream()
                .to("wordcount-output", Produced.with(Serdes.String(), Serdes.Long())); // Write the result to Kafka

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();  // Start the stream application
        System.out.println(streams.toString()); // Print the stream application topology

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close)); // we add this so that application close gracefully


    }
}
