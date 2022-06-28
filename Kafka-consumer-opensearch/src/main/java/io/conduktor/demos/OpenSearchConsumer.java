package io.conduktor.demos;

import com.google.gson.JsonParseException;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.recycler.Recycler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

//         Create an OpenSearch Client
        public static RestHighLevelClient createopenSearchClient() {
            String connString = "http://localhost:9200";

//            Build a URI from connString
        RestHighLevelClient restHighLevelClient;
        URI connURI = URI.create(connString);

//        Extract login info if it exists
        String userInfo = connURI.getUserInfo();

        if (userInfo == null) {
//            REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connURI.getHost(), connURI.getPort())));

        } else {
//            REST with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connURI.getHost(), connURI.getPort(), connURI.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                            )
            );
        }
        return restHighLevelClient;


        }
    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "consumer-opensearch-demo";

//        Creating Consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // none | earliest| latest

//        This stops consumer from committing offsets automatically
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(properties);
    }

    private static String extractId(String json) {
//            gson Library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {
            Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
//        Create opensearch client
        RestHighLevelClient openSearchClient = createopenSearchClient();



//        Create kafka client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();


//      We must create index on open search if doesnt already exist
        try(openSearchClient; consumer) {

            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Created the wikimedia index");
            } else {
                log.info("The Wikimedia Index already exists");
            }

            consumer.subscribe(Collections.singleton("wikimedia.recentchanges"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();

                log.info("received: " + recordCount + " records");

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {
//                    Trying to achieve exactly once for consumer offset reading
//                    If a consumer goes offline, we don't want it to prcess a message it already saw. So use an ID to tell it where to pick up from
//                    Strat 1 - define an ID using kafka record coordinates
//                    String id = record.topic()+ "_" + record.partition() + "_" + record.offset();


                    try {
//                        Strat 2 - extract id frm json body
                        String id = extractId(record.value());

//                    Send record into openSearch. THis does a request for every message. This is inefficient
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);
//                          THis line specifically sends it to opensearch
//                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                        bulkRequest.add(indexRequest);

                        openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

//                        log.info(response.getId());
                    } catch (Exception e) {

                    }

                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " records");

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }


                // Commit offset after batch consumed. This means we are in at least once, cause we only commit after batch is processed
                consumer.commitSync();
                log.info("Offsets have been commited");

            }

        }




//        Main logic

    }
}
