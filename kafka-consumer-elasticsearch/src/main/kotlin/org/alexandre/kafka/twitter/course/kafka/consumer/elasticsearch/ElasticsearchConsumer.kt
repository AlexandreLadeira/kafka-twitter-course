package org.alexandre.kafka.twitter.course.kafka.consumer.elasticsearch

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
            kafkaConsumer.poll(Duration.ofMillis(100)).forEach { record ->
                val indexRequest = IndexRequest(INDEX)
                    .source(record.value(), XContentType.JSON)

                val indexResponse = elasticsearchClient.index(indexRequest, RequestOptions.DEFAULT)

                logger.info(indexResponse.id)
                Thread.sleep(1000)
            }
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
            RestClient.builder(HttpHost(connUri.host, connUri.port, ELASTIC_SEARCH_SCHEMA))
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
            it.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KAFKA_AUTO_OFFSET_RESET_CONFIG)
        }

        return KafkaConsumer<String, String>(properties).also {
            it.subscribe(listOf(kafkaTopic))
        }

    }

    companion object {
        private const val ELASTIC_SEARCH_SCHEMA = "https"

        private const val INDEX = "tweets"

        private const val KAFKA_AUTO_OFFSET_RESET_CONFIG = "earliest"

        private val logger = LoggerFactory.getLogger(ElasticsearchConsumer::class.java)
    }

}
