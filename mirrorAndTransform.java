package com.company.app;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.Properties;

import org.bson.Document;

public class MirroringApp {
    public static void main(String[] args) {
    
        // Consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-server-1:9092,kafka-server-2:9092,kafka-server-3:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "NAME");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
    
        consumer.subscribe(Arrays.asList("SOURCE-TOPIC"));
    
        // Producer properties
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "kafka-server-1:9092,kafka-server-2:9092,kafka-server-3:9092");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
    
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
    
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
    
            for (ConsumerRecord<String, String> record : records) {
                String json = record.value();
    
                // Convert BSON to JSON
                Document doc = Document.parse(json);
                String jsonString = doc.toJson();
                
                // Send JSON to another Kafka topic
                producer.send(new ProducerRecord<>("DESTINATION-TOPIC", jsonString));
            }
        }
    }
}
