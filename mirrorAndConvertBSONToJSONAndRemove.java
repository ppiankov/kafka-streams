package com.company.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bson.Document;

import java.time.Duration;
import java.util.*;

public class MirroringApp {

    public static Map<String, Object> flatten(Map<String, Object> map) {
        Map<String, Object> flatMap = new HashMap<>();
        normalizeBSONTypes(map);
        flatten(map, flatMap, "");
        return flatMap;
    }

    private static void normalizeBSONTypes(Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map) {
                Map<String, Object> valueMap = (Map<String, Object>) value;
                if (valueMap.containsKey("$date")) {
                    // Handle the date and convert it to a Java Date object
                    // The actual conversion logic will depend on your date format
                    entry.setValue(new Date((Long) valueMap.get("$date")));
                } else if (valueMap.containsKey("$numberLong")) {
                    entry.setValue(Long.parseLong((String) valueMap.get("$numberLong")));
                } else if (valueMap.containsKey("$oid")) {
                    entry.setValue(valueMap.get("$oid").toString());
                } else {
                    // Recursive call for nested maps
                    normalizeBSONTypes(valueMap);
                }
            }
        }
    }

    private static void flatten(Map<String, Object> map, Map<String, Object> flatMap, String prefix) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map) {
                flatten((Map<String, Object>) value, flatMap, prefix + entry.getKey() + "_");
            } else if (value instanceof List) {
                List<?> list = (List<?>) value;
                for (int i = 0; i < list.size(); i++) {
                    if (list.get(i) instanceof Map) {
                        flatten((Map<String, Object>) list.get(i), flatMap, prefix + entry.getKey() + "_" + i + "_");
                    } else {
                        flatMap.put(prefix + entry.getKey() + "_" + i, list.get(i));
                    }
                }
            } else {
                flatMap.put(prefix + entry.getKey(), entry.getValue());
            }
        }
    }

public static void main(String[] args) throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();

    // Initialize consumer
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-server-1:9092,kafka-server-2:9092,kafka-server-3:9092");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "NAME");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
    consumer.subscribe(Arrays.asList("SOURCE-TOPIC"));

    // Initialize producer
    Properties producerProps = new Properties();
    producerProps.put("bootstrap.servers", "kafka-server-1:9092,kafka-server-2:9092,kafka-server-3:9092");
    producerProps.put("key.serializer", StringSerializer.class.getName());
    producerProps.put("value.serializer", StringSerializer.class.getName());
    KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

    while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        System.out.println("Polling records");

for (ConsumerRecord<String, String> record : records) {
    try {
        System.out.println("Received record: " + record.value());
        Document doc = Document.parse(record.value());
        String jsonString = doc.toJson();

        Map<String, Object> originalMap = objectMapper.readValue(jsonString, Map.class);

        if (originalMap.get("payload") == null) {
            System.out.println("Payload is null. Skipping record.");
            continue;
        }

        String payloadString = (String) originalMap.get("payload");
        Map<String, Object> payloadMap = objectMapper.readValue(payloadString, Map.class);

        // Flatten the Map
        Map<String, Object> flatMap = flatten(payloadMap);

	// Remove unwanted keys
        flatMap.remove("documentKey_id");
        flatMap.remove("data2");
        flatMap.remove("data3");
        flatMap.remove("data4");

        // Convert the flattened map back to JSON
        String flatJsonString = objectMapper.writeValueAsString(flatMap);

        System.out.println("Flattened and sending: " + flatJsonString);

        producer.send(new ProducerRecord<>("DESTINATION-TOPIC", flatJsonString));
    } catch (Exception e) {
        e.printStackTrace();
    }
}
    }
}

}