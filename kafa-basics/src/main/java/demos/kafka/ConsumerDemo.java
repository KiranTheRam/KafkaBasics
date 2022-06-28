package demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getName());

    public static void main(String[] args) {
        log.info("This is a consumer");

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-second-app";
        String topic = "demo_java";

//        Creating COnsumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // none | earliest| latest

//        Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

//        subscribe consumer to topics
        consumer.subscribe(Arrays.asList(topic));

//        Poll for new data
        while (true) {
            log.info("Polling ...");
//            Ask kafka for some records. Wait for 100 ms. if nothing is received, move onto next line of code. Records will be an empty list in this event
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

//            Print the records that are found
            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }


        }

    }
}
