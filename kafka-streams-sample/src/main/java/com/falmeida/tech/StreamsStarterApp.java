package com.falmeida.tech;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"stream-starter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        //1 - Stream from Kafka
        KStream<String,String> wordCountInput = builder.stream("word-count-input");
        //2 - Map Values to lowercase
        KTable<String,Long> wordCounts = wordCountInput
                .mapValues(textLine -> textLine.toLowerCase())
                //wordCountInput.mapValues(String::toLowerCase);
                //3 - flatmap values split by space
                .flatMapValues(lowercasedTextLine -> Arrays.asList(lowercasedTextLine.split("")))
                //4
                .selectKey((ignoredKey,word)-> word)
                .groupByKey()
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("Counts"));

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(),Serdes.Long()));
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

    }

}
