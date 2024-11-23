package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoWithShutDown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutDown.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Hello Kafka Consumer");
        String groupID = "my-java-application";
        String producerTopic = "demo_java_topic";

        // Steps to create a producer

        // 1. Create consumer properties

        Properties properties = new Properties();
        // Localhost Props
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        // Set Consumer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());
        properties.setProperty("group.id",groupID);
        properties.setProperty("auto.offset.reset","earliest");

        /* Value for auto.offset.rest
        *
            * None => No consumer Group
            * Earliest => from beginning
            * Latest => Latest messages
        *
        * */




        // 2. Create kafka Consumer

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // get reference to main thread

        final Thread mainThread = Thread.currentThread();

        // create a shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Shutdown Deteceted, Wakeup consumer");
                consumer.wakeup();

                // Join the main thread to allow execution of code from main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // subscribe to a topic
        try{
            consumer.subscribe(List.of(producerTopic));
            // poll data

            while(true){
                log.info("Polling");

                ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(1000)); // => Retrieve 'earliest' data from producer if it has , else wait for 1s

                for(ConsumerRecord<String,String> record: consumerRecords) {
                    log.info("Key : "  +record.key() + "," + "Value : " + record.value());
                    log.info("Partition : "  +record.partition() + "," + "Offset : " + record.offset());
                }
            }
        }catch (WakeupException e) {
            log.info("Consumer is Starting to shutdown");
        }
        catch (Exception e) {
            log.error("Unexepcted  shutdown");
        }
        finally {
            consumer.close(); // this also commits offsets
            log.info("GraceFull Shutdown of consumer");
        }
    }
}
