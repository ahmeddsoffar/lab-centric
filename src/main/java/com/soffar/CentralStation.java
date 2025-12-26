package com.soffar;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.*;

public class CentralStation {
    // Configs
    private static final String INPUT_TOPIC = "weather-readings";
    private static final String RAIN_TOPIC = "heavy-rain-alerts";
    private static final String KAFKA_SERVER = System.getenv().getOrDefault("KAFKA_SERVER", "localhost:9092");
    private static final String DB_URL = System.getenv().getOrDefault("DB_URL", "jdbc:postgresql://localhost:5432/weatherdb");

    public static void main(String[] args) throws Exception {
        System.out.println("Central Station connecting to: " + KAFKA_SERVER);

        // 1. Setup Consumerkubectl logs -l app=weather-station --tail=5
        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "central-station-group");
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps);
        consumer.subscribe(Collections.singletonList(INPUT_TOPIC));

        // 2. Setup Rain Alert Producer
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> alertProducer = new KafkaProducer<>(prodProps);

        // 3. Setup Database
        System.out.println("Waiting 10s for Database...");
        Thread.sleep(10000);
        Connection dbConn = DriverManager.getConnection(DB_URL, "postgres", "password");

        String insertSQL = "INSERT INTO weather_readings (station_id, sequence_number, battery_status, timestamp, humidity, temperature, wind_speed) VALUES (?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement insertStmt = dbConn.prepareStatement(insertSQL);

        ObjectMapper mapper = new ObjectMapper();
        List<JsonNode> buffer = new ArrayList<>();

        System.out.println("Ready. Listening for data...");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                try {
                    JsonNode node = mapper.readTree(record.value());

                    // RAIN CHECK
                    if (node.has("weather") && node.get("weather").get("humidity").asInt() > 70) {



                        int humidity = node.get("weather").get("humidity").asInt();
                        System.out.println("!!! RAIN ALERT !!! Station " + node.get("station_id") + " | Humidity: " + humidity + "%");
                        alertProducer.send(new ProducerRecord<>(RAIN_TOPIC, "rain_alert", "Heavy rain at station " + node.get("station_id")));
                    }
                    buffer.add(node);
                } catch (Exception e) { System.err.println("Error: " + e.getMessage()); }
            }

            // BATCH INSERT
            if (buffer.size() >= 5000) {
                for (JsonNode n : buffer) {
                    insertStmt.setLong(1, n.get("station_id").asLong());
                    insertStmt.setLong(2, n.get("s_no").asLong());
                    insertStmt.setString(3, n.get("battery_status").asText());
                    insertStmt.setLong(4, n.get("status_timestamp").asLong());
                    insertStmt.setInt(5, n.get("weather").get("humidity").asInt());
                    insertStmt.setInt(6, n.get("weather").get("temperature").asInt());
                    insertStmt.setInt(7, n.get("weather").get("wind_speed").asInt());
                    insertStmt.addBatch();
                }
                insertStmt.executeBatch();
                buffer.clear();
                System.out.println("Batch Saved to SQL.");
            }
        }
    }
}