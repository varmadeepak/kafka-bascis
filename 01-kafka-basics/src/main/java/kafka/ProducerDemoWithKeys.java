package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());
    public static void main(String[] args) throws InterruptedException {
        log.info("Hello Kafka Producer w/callback");

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

        for(int j=1; j<=2; j++) {
            for(int i=1; i<=10; i++){
                String topic = "demo_java_topic";
                String key = "id_" + i;
                String value = "Hello World" + i;
                ProducerRecord<String,String> producerRecord =
                        new ProducerRecord<>(topic,key,value);

                // 3. send data

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e == null) {
                            log.info("Key : " + key + "|" +" Partition : " + recordMetadata.partition());
                        }
                        else{
                            log.error("Error encountered {}",e.getMessage());
                        }
                    }
                });
            }
            Thread.sleep(500);

        }
        // 4. Flush and delete producer
//        producer.flush();
        producer.close();
    }
}
