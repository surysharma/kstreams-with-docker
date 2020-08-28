package com.thebigscale.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {

    public static void main(String[] args) {

        Properties props = getProperties();

        Producer<String, String> producer = new KafkaProducer<>(props);
          for (int i = 0; i < 10; i++) {
              sendToTopic(producer, i);
          }

        producer.close();
  }

    private static void sendToTopic(Producer<String, String> producer, int i) {
        producer.send(new ProducerRecord<>("word-count-input-topic", "c" + i), (metadata, exception) -> {
            System.out.println("Topic:" + metadata.topic());
            System.out.println("Partition:" + metadata.partition());
            System.out.println("Offset:" + metadata.offset());
        });
    }

    private static Properties getProperties() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
}
