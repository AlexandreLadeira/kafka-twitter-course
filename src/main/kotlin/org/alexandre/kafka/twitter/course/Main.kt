package org.alexandre.kafka.twitter.course

import org.alexandre.kafka.twitter.course.configuration.Configuration.elasticSearchConsumer
import org.alexandre.kafka.twitter.course.configuration.Configuration.tweetProducer

fun main() {
//    tweetProducer.run()
    elasticSearchConsumer.run()
}
