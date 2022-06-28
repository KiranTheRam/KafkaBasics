package demos.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getName());

    public static void main(String[] args) {
        log.info("This is a consumer");

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-third-app";
        String topic = "demo_java";

//        Creating Consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // none | earliest| latest
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

//        Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

//        Get a refrence to current thread
        final Thread mainThread = Thread.currentThread();

//        Adding Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup(); // Essentially forces the consumer.poll to throw exception, which stops the while loop

//                Join main thread to allow the execution of the code in main thread

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            //        subscribe consumer to topics
            consumer.subscribe(Arrays.asList(topic));

//        Poll for new data
            while (true) {
//                log.info("Polling ...");
//            Ask kafka for some records. Wait for 100 ms. if nothing is received, move onto next line of code. Records will be an empty list in this event
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

//            Print the records that are found
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }


            }
        } catch (WakeupException e) {
            log.info("Wake up exception");
//    This is an expected exception when closing a consumer
        } catch (Exception e) {
            log.error("Unexpected Exception");
        } finally {
            consumer.close(); // This will also commit offsets if need be
            log.info("Consumer has be closed properly");
        }

    }
}
