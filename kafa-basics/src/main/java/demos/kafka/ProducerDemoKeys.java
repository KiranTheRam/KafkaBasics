package demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
//    Setting up the logger
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {

//        Create Producer Properties
        Properties properties = new Properties();


        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

//        Create Producer Record &
//        Send some data - async task
        for (int i=0; i<10; i++) {

            String topic= "demo_java";
            String value="Hello Earth " + i;
            String key= "id_"+i;

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
//                executes when a record is sucessfully sent or an exception is thrown
                    if (e==null) {
                        log.info("Recieved new metadata\n" +
                                "Topic: " + metadata.topic() +
                                "\nKey: " + producerRecord.key() +
                                "\nPartition: " + metadata.partition() +
                                "\nOffset: " + metadata.offset() +
                                "\nTimestamp: " + metadata.timestamp() + "\n\n");
                    }
                    else {
                        log.error("Error while producing. ", e);
                    }
                }
            });
        }


//        Flush data - synchronous.
        producer.flush();

//        Close Producer
        producer.close();
    }
}
