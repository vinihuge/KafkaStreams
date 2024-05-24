package com.vhugenthobler;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class FavoriteColor {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> streamInput = streamsBuilder
                .stream("favorite-color-input");

        KStream<String, String> favoriteColorInput = streamInput
                .filter((key, value) -> value.contains(","))
                .mapValues(value -> value.toLowerCase().split(","))
                .filter((key, value) -> Arrays.asList("green", "red", "blue").contains(value[1]))
                .selectKey((key, value) -> value[0])
                .mapValues(value -> value[1]);

        favoriteColorInput.to("favorite-color-middle", Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, String> favoriteColorMiddle = streamsBuilder.table("favorite-color-middle");

        KTable<String, Long> colorCount = favoriteColorMiddle
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count();

        colorCount.toStream().to("favorite-color-output", Produced.with(Serdes.String(), Serdes.Long()));


        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
