package com.consumer;

import com.station.Station;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.google.gson.Gson;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class BikeStationConsumer {
    public static void main(String[] args) {
        String topic = "empty-stations";
        String groupId = "monitor-stations";
        String bootstrapServers = "localhost:9092, localhost:9093";

        // Set consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create consumer
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // Subscribe to topic
            consumer.subscribe(Collections.singletonList(topic));

            // Continuously poll for new messages
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                // Process each record
                records.forEach(record -> {
                    try {
                        Gson gson = new Gson();
                        Station station = gson.fromJson(record.value(), Station.class);
                        if (station.isEmpty()) {
                            System.out.printf("The Station of %s %s is empty in this moment !\n",
                                    station.getContract_name(), station.getName());
                        }
                    } catch (Exception e) {
                        System.err.println("Error parsing JSON: " + e.getMessage());
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
