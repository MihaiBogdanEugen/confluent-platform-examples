package de.mls.confluent.platform.examples;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SimpleProducerExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleProducerExample.class);
    private static final int NoOfRecordsToSend = 100;
    private static final Map<String, List<Integer>> TopicsAndPartitions = new HashMap<>();
    private static final String[] KafkaServers = new String[] {
            "localhost:19092",
            "localhost:29092",
            "localhost:39092",
            "localhost:49092",
            "localhost:59092",
    };

    static {
        TopicsAndPartitions.put("simplemessages1", Arrays.asList(0, 1, 2, 3, 4));
        TopicsAndPartitions.put("simplemessages2", Arrays.asList(0, 1, 2, 3, 4));
        TopicsAndPartitions.put("simplemessages3", Arrays.asList(0, 1, 2, 3, 4));
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(", ", KafkaServers));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return properties;
    }

    public static void main(String[] args) {

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties())) {

            for (int index = 0; index < NoOfRecordsToSend; index++) {
                for (Map.Entry<String, List<Integer>> entry : TopicsAndPartitions.entrySet()) {

                    int noOfPartitions = entry.getValue().size();
                    String topic = entry.getKey();
                    Integer partition = index % noOfPartitions;
                    String key = UUID.randomUUID().toString();
                    String value = String.format("Message #%d", index);

                    ProducerRecord<String, String> record = (noOfPartitions == 1)
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