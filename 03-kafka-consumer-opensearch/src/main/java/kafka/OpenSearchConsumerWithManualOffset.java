package kafka;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumerWithManualOffset {

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupID = "consumer-opensearch-demo";
//        String producerTopic = "demo_java_topic";

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
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");

        return new KafkaConsumer<>(properties);
    }
    private static String extractIdFromMetaJson(String value) {
        return JsonParser.parseString(value)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }
    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumerWithManualOffset.class.getName());
        // create open search client

        RestHighLevelClient openSearchClient = createOpenSearchClient();
        KafkaConsumer<String,String> kafkaConsumer = createKafkaConsumer();
        final Thread mainThread = Thread.currentThread();

        // create a shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Shutdown Deteceted, Wakeup consumer");
                kafkaConsumer.wakeup();

                // Join the main thread to allow execution of code from main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // create index for open saech if it doesnt exist
        try (openSearchClient; kafkaConsumer){
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"),RequestOptions.DEFAULT);
            if(indexExists) {
                log.info("Wikimedia Index Already Exists");
            }
            else{
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Wikimedia index created");
            }
            kafkaConsumer.subscribe(Collections.singleton("wikimedia-recentchange"));


            while(true){
                ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                BulkRequest bulkRequest = new BulkRequest();
                log.info("Received {} records",recordCount);

                for(ConsumerRecord<String,String> record : records) {
                    // send record into open search

                    // Make consumer Idempotent

                    // Strategy 1 => define ID using Record coordinates
                    //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    // Better strategy : use id provided by wikimedia in meta
                    String id = extractIdFromMetaJson(record.value());


                    IndexRequest indexRequest = new IndexRequest("wikimedia")
                            .source(record.value(), XContentType.JSON)
                            .id(id);
                    bulkRequest.add(indexRequest);
//                    IndexResponse response = openSearchClient.index(indexRequest,RequestOptions.DEFAULT);
//                    log.info("Inserted 1 document into opensearch with id : {}",response.getId());

                }
                // commit offset manually
                if(bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponses = openSearchClient.bulk(bulkRequest,RequestOptions.DEFAULT);
                    log.info("Inserted Items : {}",bulkResponses.getItems().length);

                    try {
                        Thread.sleep(1000);
                    }catch (Exception ignored){

                    }

                }

                kafkaConsumer.commitSync();
                log.info("Offsets have been committed");
            }

        }catch (WakeupException e) {
            log.info("Consumer is Starting to shutdown");
        }
        catch (Exception e) {
            log.error("Unexepcted  shutdown");
        }
        finally {
            kafkaConsumer.close(); // this also commits offsets
            openSearchClient.close();
            log.info("GraceFull Shutdown of consumer");
        }
    }
        // create our kafka client


        // main code logic

        // close things

}





