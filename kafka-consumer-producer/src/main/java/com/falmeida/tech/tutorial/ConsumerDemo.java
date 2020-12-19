package com.falmeida.tech.tutorial;

import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "groupId-first";
        String topic = "first_topic";

        //consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        //subscribe consumer
        //consumer.subscribe(Collections.singleton(topic));
        //consumer.subscribe(Arrays.asList("topic1","topic2","topic3"));
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
        for(ConsumerRecord<String,String> record: records){
            logger.info("Key: " + record.key() + ", Value: " + record.value());
            logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
        }
    }

}
