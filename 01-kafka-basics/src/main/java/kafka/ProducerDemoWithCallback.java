package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
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
        properties.setProperty("batch.size","400");
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());



        // 2. Create kafka producer

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        // Create a producer record

       for(int j=1; j<=10; j++) {
           for(int i=1; i<=30; i++){
               ProducerRecord<String,String> producerRecord =
                       new ProducerRecord<>("demo_java_topic","Hello World" + i);

               // 3. send data

               producer.send(producerRecord, new Callback() {
                   @Override
                   public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                       if(e == null) {
                           log.info("Received metadata \n" +
                                   "Topic : " + recordMetadata.topic() + "\n" +
                                   "Partition : " + recordMetadata.partition() + "\n" +
                                   "Offset : " + recordMetadata.offset() + "\n" +
                                   "Timestamp : " + recordMetadata.timestamp() + "\n");
                       }
                       else{
                           log.error("Error encountered {}",e.getMessage());
                       }
                   }
               });

               Thread.sleep(500);
           }
       }

        // 4. Flush and delete producer
//        producer.flush();
        producer.close();
    }
}
