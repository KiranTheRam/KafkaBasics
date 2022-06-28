package demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
//    Setting up the logger
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {

//        Create Producer Properties
        Properties properties = new Properties();
    //        This is the typical layout for setting a property
//        properties.setProperty("key", "value");
    //        These values are needed but don't have to be hard coded like this. Use the ProducerConfig
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
//        properties.setProperty("key.serializer", "");
//        properties.setProperty("value.serializer", "");

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

//        Create Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");

//        Send some data - async task
        producer.send(producerRecord);

//        Flush data - synchronous.
        producer.flush();

//        Close Producer. THe .close will also flush for you, but just as a fyi the flush method exists
        producer.close();
    }
}
