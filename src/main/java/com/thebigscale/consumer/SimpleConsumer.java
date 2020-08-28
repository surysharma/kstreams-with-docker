package com.thebigscale.consumer;

import com.thebigscale.producer.SimpleProducerWithKeys;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SimpleConsumer {
    private static Logger log = LoggerFactory.getLogger(SimpleConsumer.class);

    public static void main(String[] args) throws InterruptedException {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "wordcount-example-1");

        var consumer = new KafkaConsumer(properties);
        consumer.subscribe(Arrays.asList("word-count-input-topic"));

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.submit(() -> {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            consumerRecords.forEach(record -> log.info(String.format("Key: %s, value: %s, partition:%s, offset:%s", record.key(),
                    record.value(), record.partition(), record.offset())));

        });

        Runtime.getRuntime().addShutdownHook(new Thread());
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }
}
