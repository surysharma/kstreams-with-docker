package com.thebigscale.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class SimpleProducerWithTwitterSrc {
    private static Logger log = LoggerFactory.getLogger(SimpleProducerWithTwitterSrc.class);
    private static String TOPIC_NAME = "twitter-input-topic";

    public static void main(String[] args) {

        try {
            /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
            BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
            Client client = connectToClient(() -> new StringDelimitedProcessor(msgQueue),
                    Lists.newArrayList("#cbiforssr"));

            Producer<String, String> producer = getProducer();
            // on a different thread, or multiple different threads....
            while (!client.isDone()) {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                sendToKafka(msg, producer);
            }
            Runtime.getRuntime().addShutdownHook(new Thread(()-> {
                log.info("Stopping client...");
                client.stop();
                log.info("Stopping producer...");
                producer.close();
            }));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
  }

    private static <T, R> Client connectToClient(Supplier<StringDelimitedProcessor> fn, List<String> hastags) throws InterruptedException {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(hastags);

        // These secrets should be read from a config file. These need to be regenerated again.
        Authentication hosebirdAuth = new OAuth1(
                "23y4EvMlRpkbphToU3T7csKD2", //These are invalidated!
                "frhT6GfnmsxnXzLfl4x1xHcg7RtqAp7jWKRntqlxIYRWYS968y",
                "61999980-x9ErHJvF8uZlXEba44xH77Ok0uBT5TkXCf4zt90bH",
                "C1xpA68KLU8onj6FfFFH18NKypn7n1IHyzmdFIHFu9T25");

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(fn.get());

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
        hosebirdClient.connect();
        return hosebirdClient;
    }

    public static void sendToKafka(String tweet, Producer producer) {
        sendToTopic(producer, tweet);
    }

    private static Producer<String, String> getProducer() {
        log.info("Starting...");
        Properties props = getProperties();

        Producer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }


    private static void sendToTopic(Producer<String, String> producer, String tweet) {
        try {
            producer.send(new ProducerRecord<>(TOPIC_NAME, tweet), (recordMetadata, exception) -> {
                if (exception == null) {
                    log.info(String.format("After response Topic: %s, tweet:%s, partition: %s, offset: %s",
                            recordMetadata.topic(),
                            tweet,
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

        //The below setting makes as very high Throughput, optimal Producer!
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        return props;
    }
}
