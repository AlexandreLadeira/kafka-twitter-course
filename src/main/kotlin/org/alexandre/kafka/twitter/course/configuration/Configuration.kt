package org.alexandre.kafka.twitter.course.configuration

import com.typesafe.config.ConfigFactory
import org.alexandre.kafka.twitter.course.kafka.consumer.elasticsearch.ElasticsearchConsumer
import org.alexandre.kafka.twitter.course.kafka.twitter.producer.KafkaTweetProducer

object Configuration {
    private const val APPLICATION = "kafka-twitter-course"

    private val config = ConfigFactory.load().getConfig(APPLICATION)

    private val kafkaBootstrapServer = config.getString("kafka.bootstrap-server")
    private val kafkaTopic = config.getString("kafka.topic")

    val tweetProducer = KafkaTweetProducer(
        twitterConsumerKey = config.getString("twitter.consumer-key"),
        twitterConsumerSecret = config.getString("twitter.consumer-secret"),
        twitterToken = config.getString("twitter.token"),
        twitterTokenSecret = config.getString("twitter.token-secret"),
        kafkaBootstrapServer = kafkaBootstrapServer,
        kafkaTopic = kafkaTopic,
        termsToTrack = config.getStringList("twitter.terms-to-track")
    )

    val elasticSearchConsumer = ElasticsearchConsumer(
        elasticsearchURL = config.getString("elasticsearch.url"),
        kafkaBootstrapServer = kafkaBootstrapServer,
        kafkaConsumerGroupId = config.getString("kafka.consumer-group-id"),
        kafkaTopic = kafkaTopic
    )
}
