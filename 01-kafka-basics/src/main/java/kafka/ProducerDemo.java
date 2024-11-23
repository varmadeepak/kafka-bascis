package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Hello Kafka Producer");

        // Steps to create a producer

        // 1. Create Produce properties

        Properties properties = new Properties();
        // Localhost Props
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        // Virtual Secure cluster

//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
//        properties.setProperty("security.protocol","SASL_SSL");
//        properties.setProperty("sasl.jas.config","127.0.0.1:9092");
//        properties.setProperty("sasl.mechanism","PLAIN");

        // Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());



        // 2. Create kafka producer

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        // Create a producer record

        ProducerRecord<String,String> producerRecord =
                new ProducerRecord<>("demo_java_topic","Hello World");

        // 3. send data

        producer.send(producerRecord);

        // 4. Flush and delete producer
//        producer.flush();
        producer.close();
    }
}
