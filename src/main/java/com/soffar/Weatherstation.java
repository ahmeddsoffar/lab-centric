package com.soffar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.Random;

public class Weatherstation {
    private static final String TOPIC = "weather-readings";
    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("KAFKA_SERVER", "localhost:9092");
    private static final Random random = new Random();
    // Generate a random Station ID between 1 and 100 on startup
    private static final long STATION_ID = Math.abs(random.nextLong() % 100) + 1;

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();
        long sequenceNumber = 0;

        System.out.println("Starting Station ID: " + STATION_ID + " connecting to " + BOOTSTRAP_SERVERS);

        while (true) {
            sequenceNumber++;
            // 1. Simulate 10% Drop Rate (Network Packet Loss)
            if (random.nextDouble() < 0.10) {
                System.out.println("Station " + STATION_ID + ": Message dropped (simulated loss).");
                Thread.sleep(1000);
                continue;
            }

            // 2. Generate Battery Status
            String batteryStatus;
            double batRand = random.nextDouble();
            if (batRand < 0.3) batteryStatus = "low";
            else if (batRand < 0.7) batteryStatus = "medium";
            else batteryStatus = "high";

            // 3. Construct JSON
            ObjectNode root = mapper.createObjectNode();
            root.put("station_id", STATION_ID);
            root.put("s_no", ++sequenceNumber);



            root.put("battery_status", batteryStatus);
            root.put("status_timestamp", System.currentTimeMillis() / 1000);

            ObjectNode weather = root.putObject("weather");
            weather.put("humidity", random.nextInt(100));
            weather.put("temperature", random.nextInt(100));
            weather.put("wind_speed", random.nextInt(100));

            String jsonString = root.toString();

            // 4. Send to Kafka
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, String.valueOf(STATION_ID), jsonString);
            producer.send(record);

            System.out.println("Station " + STATION_ID + " Sent: " + jsonString);
            Thread.sleep(1000);
        }
    }
}