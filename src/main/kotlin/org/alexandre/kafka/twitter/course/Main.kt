package org.alexandre.kafka.twitter.course

import org.alexandre.kafka.twitter.course.configuration.Configuration.elasticSearchConsumer
import org.alexandre.kafka.twitter.course.configuration.Configuration.tweetProducer

fun main() {
    Thread { tweetProducer.run() }.start()
    Thread { elasticSearchConsumer.run() }.start()
}
