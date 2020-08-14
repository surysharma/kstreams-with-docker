import io.confluent.common.utils.TestUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class WordCountExample {

    private static final String CONSUMER_GROUP = "wordcount-example";
    private static final String OUTPUT_TOPIC = "word-count-output-topic";
    private static String INPUT_TOPIC = "word-count-input-topic";

    public static void main(String[] args) {
        //Define a configuration
        var configuration = getConfiguration();

        //Define a streamBuilder and define a processing topology
        var builder = new StreamsBuilder();
        createWordCountStream(builder);
        var streams = new KafkaStreams(builder.build(), configuration);

        //Reset the stream
        streams.cleanUp();

        //start the topology
        streams.start();

        //Add shutdown hook to close the streams gracefully.
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static void createWordCountStream(StreamsBuilder builder) {
        KStream<String, String> textStream = builder.stream(INPUT_TOPIC);
        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        KTable<String, Long> wordCount = textStream
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .peek((k, v) -> System.out.println("Key is k:" + k + " value is v:" + v))
                .groupBy((ignore, word) -> word)
                .count();

        wordCount.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

    public static Properties getConfiguration() {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, CONSUMER_GROUP);
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-example-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        // For illustrative purposes we disable record caches.
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // Use a temporary directory for storing state, which will be automatically removed after the test.
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        return streamsConfiguration;
    }
}
