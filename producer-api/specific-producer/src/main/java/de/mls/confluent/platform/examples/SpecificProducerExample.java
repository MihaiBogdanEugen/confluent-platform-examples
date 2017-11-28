package de.mls.confluent.platform.examples;

import de.mls.confluent.platform.examples.avro.LogLine;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SpecificProducerExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpecificProducerExample.class);
    private static final Random RANDOM = new Random();
    private static final int NoOfRecordsToSend = 100;
    private static final Map<String, List<Integer>> TopicsAndPartitions = new HashMap<>();
    private static final String SchemaRegistryUrl = "http://localhost:8081";
    private static final String[] KafkaServers = new String[] {
            "localhost:19092",
            "localhost:29092",
            "localhost:39092",
            "localhost:49092",
            "localhost:59092",
    };

    static {
        TopicsAndPartitions.put("specificmessages1", Arrays.asList(0, 1, 2, 3, 4));
        TopicsAndPartitions.put("specificmessages2", Arrays.asList(0, 1, 2, 3, 4));
        TopicsAndPartitions.put("specificmessages3", Arrays.asList(0, 1, 2, 3, 4));
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(", ", KafkaServers));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        properties.put("schema.registry.url", SchemaRegistryUrl);
        return properties;
    }

    private static LogLine getRandomSpecificRecord() {

        final String[] websites = {"support.html","about.html","foo.html", "bar.html", "home.html", "search.html", "list.html", "help.html", "bar.html", "foo.html"};

        LogLine logLine = new LogLine();

        logLine.setIp("66.249.1."+ RANDOM.nextInt(10));
        logLine.setReferrer("http://www.marketlogicsoftware.com/");
        logLine.setTimestamp(new Date().getTime());
        logLine.setUrl(websites[RANDOM.nextInt(websites.length)]);
        logLine.setUseragent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.125 Safari/537.36");

        return logLine;
    }

    public static void main(String[] args){

        try (KafkaProducer<String, LogLine> producer = new KafkaProducer<>(getProperties())) {

            for (int index = 0; index < NoOfRecordsToSend; index++) {
                for (Map.Entry<String, List<Integer>> entry : TopicsAndPartitions.entrySet()) {

                    int noOfPartitions = entry.getValue().size();
                    String topic = entry.getKey();
                    Integer partition = index % noOfPartitions;
                    String key = UUID.randomUUID().toString();
                    LogLine value = getRandomSpecificRecord();

                    ProducerRecord<String, LogLine> record = (noOfPartitions == 1)
                            ? new ProducerRecord<>(topic, key, value)
                            : new ProducerRecord<>(topic, partition, key, value);

                    RecordMetadata recordMetadata = producer.send(record).get();

                    LOGGER.info(String.format("Record with value [%s] is sent to topic.partition [%s.%d] with offset %d",
                            record.value(), recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()));
                }
            }
        } catch (Exception error) {
            LOGGER.error(error.getMessage(), error);
        }
        LOGGER.info("DONE!");
    }
}
