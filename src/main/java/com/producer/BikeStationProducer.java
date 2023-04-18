package com.producer;

import java.net.URL;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;

public class BikeStationProducer {

    private static final String API_KEY = "5c28e98285b61d63dc0069132031d887dc4afcaa";
    private static final String URL = "https://api.jcdecaux.com/vls/v1/stations?apiKey=" + API_KEY;

    private static JSONArray getStationsData() {
        JSONArray stations = null;
        try {
            URL apiURL = new URL(BikeStationProducer.URL);
            String jsonData = new String(apiURL.openStream().readAllBytes());
            stations = new JSONArray(jsonData);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return stations;
    }
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            JSONArray previousStations = getStationsData();

            while (true) {
                JSONArray stations = getStationsData();
                int nbStations = 0;
                for (int i = 0; i < stations.length(); i++) {
                    JSONObject station = stations.getJSONObject(i);
                    JSONObject previousStation = previousStations.getJSONObject(i);
                    if (station.getInt("available_bikes") == 0 && previousStation.getInt("available_bikes") >= 1) {
                        station.put("empty", true);
                        ProducerRecord<String, String> record = new ProducerRecord<>("empty-stations",
                                Integer.toString(station.getInt("number")), station.toString());
                        producer.send(record).get();
                        nbStations++;
                    }
                    if (station.getInt("available_bikes") >= 1 && previousStation.getInt("available_bikes") == 0) {
                        station.put("empty", false);
                        ProducerRecord<String, String> record = new ProducerRecord<>("empty-stations",
                                Integer.toString(station.getInt("number")), station.toString());
                        producer.send(record).get();
                        nbStations++;
                    }
                }
                previousStations = stations;
                System.out.printf("%s Produced %d station records%n",
                        java.time.LocalDateTime.now(), nbStations);
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
