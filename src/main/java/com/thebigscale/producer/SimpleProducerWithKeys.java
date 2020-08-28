package com.thebigscale.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerWithKeys {
    private static Logger log = LoggerFactory.getLogger(SimpleProducerWithKeys.class);

    public static void main(String[] args) {

        log.info("Starting...");
        Properties props = getProperties();

        Producer<String, String> producer = new KafkaProducer<>(props);
          for (int i = 0; i < 10; i++) {
              sendToTopic(producer, i);
          }

        producer.close();
  }

    private static void sendToTopic(Producer<String, String> producer, int i) {
        try {
            String key = "key_" + i;
            String value = "j" + i;
            producer.send(new ProducerRecord<>("word-count-input-topic", key, value), (recordMetadata, exception) -> {
                if (exception == null) {
                    log.info(String.format("After response Topic: %s, (key:%s|value:%s), partition: %s, offset: %s",
                            recordMetadata.topic(),
                            key,
                            value,
                            recordMetadata.partition(),
                            recordMetadata.offset()));
                }
            }).get();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
}
