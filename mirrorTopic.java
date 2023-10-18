package com.company.app;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.common.serialization.Serde;

import java.util.Properties;

public class MirroringApp {

    public static void main(String[] args) {
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer());

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mirroringing-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-address:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, jsonSerde.getClass().getName());


        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> source = builder.stream("SOURCE-TOPIC", Consumed.with(Serdes.String(), jsonSerde));
        source.to("DESTINATION-TOPIC");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add a shutdown hook to properly close the streams application.
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}