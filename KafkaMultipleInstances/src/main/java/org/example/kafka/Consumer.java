package org.example.kafka;

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

public class Consumer {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(Producer.class);

        final String bootstrapServers = "127.0.0.1:9092";
        final String consumerGroupId = "java-group-consumer";

//Creating the properties object for consumer
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,consumerGroupId);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Creating the consumer

        final KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(consumerProperties);

        //Subscribe to topic
        consumer.subscribe(Arrays.asList("kafkaTopic"));

        //Poll and consume records
        while (true){
           ConsumerRecords<String, String> records=  consumer.poll(Duration.ofMillis(1000));
           for(ConsumerRecord record : records){
               logger.info("Received new record: \n" +
                       "Key: " + record.key() + ", " +
                               "Value: " + record.value() + ", " +
                       "Topic: " + record.topic() + ", " +
                               "Partition: " + record.partition() + ", " +
                               "Offset: " + record.offset() + "\n"
                       );
           }
        }
    }
}
