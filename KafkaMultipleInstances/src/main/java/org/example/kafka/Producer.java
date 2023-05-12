package org.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(Producer.class);

        //Creating the properties object for the producer
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //Creating the producer
        final KafkaProducer<String,String> producer = new KafkaProducer<String, String>(producerProperties);


    for(int i=0; i<10; ++i) {
    //Creating the producerRecord to be sent to the kafka topic
    ProducerRecord<String,String> record = new ProducerRecord<>("kafkaTopic", "key1" + i, "hey! please run!_" + i);


    //Sending the data to topic
    producer.send(record, new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if(e==null){
                logger.info("\nReceived record metadata. \n"+
                        "Topic " + recordMetadata.topic() + ", Partition: " + recordMetadata.partition() + ", "+
                        "Offset" + recordMetadata.offset() + "@Timestamp " + recordMetadata.timestamp() + "\n"
                );

            }else{
                logger.error("Error occurred!", e);

            }
        }
    });
}
        // flush and close the producer. flush writes any pending record that the producer must be having into the topic.

        producer.flush();
        producer.close();

    }
}
