package org.alexandre.kafka.twitter.course.kafka.consumer.elasticsearch

import com.google.gson.JsonParser
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory
import java.net.URI
import java.time.Duration
import java.util.*


class ElasticsearchConsumer(
    private val elasticsearchURL: String,
    private val kafkaBootstrapServer: String,
    private val kafkaConsumerGroupId: String,
    private val kafkaTopic: String
) {
    fun run() {
        val elasticsearchClient = createClient()
        val kafkaConsumer = createKafkaConsumer()

        while (true) {
            kafkaConsumer.poll(Duration.ofMillis(100))
                .also { logger.info("Received ${it.count()} records") }
                .forEach { record ->
                    val tweet = record.value()
                    val indexRequest = IndexRequest(ELASTIC_SEARCH_INDEX)
                        .id(tweet.extractTweetId())
                        .source(tweet, XContentType.JSON)

                    val indexResponse = elasticsearchClient.index(indexRequest, RequestOptions.DEFAULT)

                    logger.info(indexResponse.id)
                    Thread.sleep(1000)
                }

            logger.info("Committing the offsets")
            kafkaConsumer.commitSync()
            logger.info("Offsets committed")
            Thread.sleep(1000)
        }
    }

    private fun createClient(): RestHighLevelClient {
        val connUri = URI.create(elasticsearchURL)
        val (username, password) = connUri.userInfo.split(":")

        val credentialsProvider = BasicCredentialsProvider().also {
            it.setCredentials(
                AuthScope.ANY,
                UsernamePasswordCredentials(username, password)
            )
        }

        return RestHighLevelClient(
            RestClient.builder(HttpHost(connUri.host, connUri.port, ELASTIC_SEARCH_SCHEME))
                .setHttpClientConfigCallback { httpAsyncClientBuilder ->
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                        .setKeepAliveStrategy(DefaultConnectionKeepAliveStrategy())
                }
        )
    }

    private fun createKafkaConsumer(): KafkaConsumer<String, String> {
        val properties = Properties().also {
            it.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer)
            it.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
            it.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
            it.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerGroupId)
            it.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KAFKA_AUTO_OFFSET_RESET)
            it.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, KAFKA_ENABLE_AUTO_COMMIT)
            it.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, KAFKA_MAX_POLL_RECORDS)
        }

        return KafkaConsumer<String, String>(properties).also {
            it.subscribe(listOf(kafkaTopic))
        }
    }

    private fun String.extractTweetId(): String = JsonParser.parseString(this)
        .asJsonObject
        .get(TWEET_ID)
        .asString

    companion object {
        private const val ELASTIC_SEARCH_SCHEME = "https"
        private const val ELASTIC_SEARCH_INDEX = "tweets"

        private const val KAFKA_AUTO_OFFSET_RESET = "earliest"
        private const val KAFKA_ENABLE_AUTO_COMMIT = "false"
        private const val KAFKA_MAX_POLL_RECORDS = "10"


        private const val TWEET_ID = "id_str"

        private val logger = LoggerFactory.getLogger(ElasticsearchConsumer::class.java)
    }

}
