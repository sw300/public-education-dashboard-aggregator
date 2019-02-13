package com.sw300.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;

public class EnrollmentAggregate {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "enrollment-aggregate-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "education-kafka:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();
        // 1 - stream from Kafka

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer); //Serder 묶음을 만드는거네..


        KStream<String, JsonNode> textLines = builder.stream(Serdes.String(), jsonSerde, "class.topic");


        ObjectNode initialTotal= JsonNodeFactory.instance.objectNode();
        initialTotal.put("count", 0);
        initialTotal.put("totalPrice", 0);
        initialTotal.put("totalTime", 0);


        KTable<String, JsonNode> enrollments = textLines
                .groupByKey(Serdes.String(), jsonSerde)  //키 값으로 묶어주네
               // .map((key, value) -> new KeyValue<>(value, value))
                .aggregate(
                        () -> initialTotal,  // 여기서부터 시작인감?
                        (key, enrollment, total) -> newTotal(enrollment, total), //aggregation function
                        jsonSerde, // serder
                        "bank-balance-agg" +  // 이건뭔지......
                                "" +
                                "" +
                                ""
                );




        // 7 - to in order to write the results back to kafka
        enrollments.to(Serdes.String(), jsonSerde, "enrollment-total-output");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();


        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }


    private static JsonNode newTotal(JsonNode enrollment, JsonNode total) {
        // create a new balance json object
        ObjectNode newTotal = JsonNodeFactory.instance.objectNode();
        newTotal.put("count", total.get("count").asInt() + 1);
        newTotal.put("totalPrice", total.get("totalPrice").asInt() + enrollment.get("price").asInt());
        newTotal.put("totalTime", total.get("totalTime").asInt() + enrollment.get("hour").asInt());

        return newTotal;
    }
}
