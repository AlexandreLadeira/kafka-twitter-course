package org.alexandre.kafka.twitter.course.configuration

import com.typesafe.config.ConfigFactory
import org.alexandre.kafka.twitter.course.KafkaTweetProducer

object Configuration {
    private const val APPLICATION = "kafka-twitter-course"

    private val config = ConfigFactory.load().getConfig(APPLICATION)

    val twitterProducer = KafkaTweetProducer(
        twitterConsumerKey = config.getString("twitter.consumer-key"),
        twitterConsumerSecret = config.getString("twitter.consumer-secret"),
        twitterToken = config.getString("twitter.token"),
        twitterTokenSecret = config.getString("twitter.token-secret"),
        kafkaBootstrapServer = config.getString("kafka.bootstrap-server"),
        kafkaTopic = config.getString("kafka.topic"),
        termsToTrack = config.getStringList("twitter.terms-to-track")
    )
}
