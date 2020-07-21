package org.alexandre.kafka.twitter.course.kafka.consumer.elasticsearch

import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestClientBuilder
import org.elasticsearch.client.RestHighLevelClient


class ElasticSearchConsumer(
    private val elasticSearchHostName: String,
    private val elasticsSearchUsername: String,
    private val elasticSearchPassword: String
) {
    fun createClient(): RestHighLevelClient? {
        val credentialsProvider = BasicCredentialsProvider()
        credentialsProvider.setCredentials(
            AuthScope.ANY,
            UsernamePasswordCredentials(elasticsSearchUsername, elasticSearchPassword)
        )
        val builder: RestClientBuilder = RestClient.builder(
            HttpHost(elasticSearchHostName, ELASTIC_SEARCH_PORT, ELASTIC_SEARCH_SCHEMA)
        )
            .setHttpClientConfigCallback { httpClientBuilder ->
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
            }
        return RestHighLevelClient(builder)
    }

    companion object {
        private const val ELASTIC_SEARCH_PORT = 443
        private const val ELASTIC_SEARCH_SCHEMA = "https"
    }

}
