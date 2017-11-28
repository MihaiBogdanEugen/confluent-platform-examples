package de.mls.confluent.platform.examples;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class SimpleConsumerExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumerExample.class);
    private static final int NoOfRecordsToReceive = 100;
    private static final int PollingTimeoutMilliseconds = 1000;
    private static final Map<String, List<Integer>> TopicsAndPartitions = new HashMap<>();
    private static final String[] KafkaServers = new String[] {
            "localhost:19092",
            "localhost:29092",
            "localhost:39092",
            "localhost:49092",
            "localhost:59092",
    };
    private static final boolean DoSubscribe = false;
    private static final boolean DoAutoCommit = true;

    static {
        TopicsAndPartitions.put("simplemessages1", Arrays.asList(0, 1, 2, 3, 4));
        TopicsAndPartitions.put("simplemessages2", Arrays.asList(0, 1, 2, 3, 4));
        TopicsAndPartitions.put("simplemessages3", Arrays.asList(0, 1, 2, 3, 4));
    }

    private static Collection<String> getAllTopics() {
        return TopicsAndPartitions.keySet();
    }

    private static Collection<TopicPartition> getAllTopicPartitions() {
        return TopicsAndPartitions.entrySet()
                .stream()
                .flatMap(entry -> entry.getValue()
                        .stream()
                        .map(partition -> new TopicPartition(entry.getKey(), partition)))
                .collect(Collectors.toList());
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(", ", KafkaServers));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, DoAutoCommit);
        return properties;
    }

    public static void main(String[] args) {

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProperties())) {

            if (DoSubscribe) {
                Collection<String> topics = getAllTopics();
                consumer.subscribe(topics);
            } else {
                Collection<TopicPartition> topicPArtitions = getAllTopicPartitions();
                consumer.assign(topicPArtitions);
                consumer.seekToBeginning(topicPArtitions);
            }

            int counterMessagesReceived = 0;

            while(true) {

                ConsumerRecords<String, String> records =  consumer.poll(PollingTimeoutMilliseconds);
                for (ConsumerRecord<String, String> record : records) {

                    LOGGER.info(String.format("Record with value [%s] is received from topic.partition [%s.%d] with offset %d",
                            record.value(), record.topic(), record.partition(), record.offset()));

                    if (!DoAutoCommit) {
                        consumer.commitAsync(((offsets, error) -> LOGGER.error(error.getMessage(), error)));
                    }

                    counterMessagesReceived++;
                }

                if (counterMessagesReceived >= NoOfRecordsToReceive) {
                    break;
                }
            }

            if (DoSubscribe) {
                consumer.unsubscribe();
            }
        } catch (Exception error) {
            LOGGER.error(error.getMessage(), error);
        }
        LOGGER.info("DONE!");
    }
}