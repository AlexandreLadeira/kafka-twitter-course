package org.alexandre.kafka.twitter.course.configuration

import com.typesafe.config.ConfigFactory
import org.alexandre.kafka.twitter.course.TwitterProducer

object Configuration {
    private const val APPLICATION = "kafka-twitter-course"

    private val config = ConfigFactory.load().getConfig(APPLICATION)

    val twitterProducer = TwitterProducer(
        consumerKey = config.getString("twitter.consumer-key"),
        consumerSecret = config.getString("twitter.consumer-secret"),
        token = config.getString("twitter.token"),
        tokenSecret = config.getString("twitter.token-secret")
    )
}
