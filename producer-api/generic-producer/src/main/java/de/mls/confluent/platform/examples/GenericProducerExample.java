package de.mls.confluent.platform.examples;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GenericProducerExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenericProducerExample.class);
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
        TopicsAndPartitions.put("genericmessages1", Arrays.asList(0, 1, 2, 3, 4));
        TopicsAndPartitions.put("genericmessages2", Arrays.asList(0, 1, 2, 3, 4));
        TopicsAndPartitions.put("genericmessages3", Arrays.asList(0, 1, 2, 3, 4));
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

    private static GenericRecord getRandomGenericRecord() {

        final String schemaString = "{\"namespace\": \"de.mls.confluent.platform.examples.avro\", \"type\": \"record\", " +
                "\"name\": \"PageVisit\"," +
                "\"fields\": [" +
                "{\"name\": \"time\", \"type\": \"long\"}," +
                "{\"name\": \"site\", \"type\": \"string\"}," +
                "{\"name\": \"ip\", \"type\": \"string\"}" +
                "]}";

        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(schemaString);

        GenericRecord pageVisit = new GenericData.Record(schema);
        pageVisit.put("time", new Date().getTime());
        pageVisit.put("site", "http://www.marketlogicsoftware.com/");
        pageVisit.put("ip", "192.168.1." + RANDOM.nextInt(255));

        return pageVisit;
    }

    public static void main(String[] args){

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(getProperties())) {

            for (int index = 0; index < NoOfRecordsToSend; index++) {
                for (Map.Entry<String, List<Integer>> entry : TopicsAndPartitions.entrySet()) {

                    int noOfPartitions = entry.getValue().size();
                    String topic = entry.getKey();
                    Integer partition = index % noOfPartitions;
                    String key = UUID.randomUUID().toString();
                    GenericRecord value = getRandomGenericRecord();

                    ProducerRecord<String, GenericRecord> record = (noOfPartitions == 1)
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