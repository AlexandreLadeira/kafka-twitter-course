package org.alexandre.kafka.twitter.course.kafka.streams.filter.tweets

import com.google.gson.JsonParser
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import java.util.*


class StreamFilterTweets {
    fun run() {
        val properties = Properties()
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams")
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde::class.qualifiedName)
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde::class.qualifiedName)

        val streamsBuilder = StreamsBuilder()

        val inputTopic = streamsBuilder.stream<String, String>("twitter_tweets")
        val filteredStream = inputTopic.filter { _, jsonTweet ->
            jsonTweet.tweetFollowersCount > 10000
        }
        filteredStream.to("important_tweets")

        val kafkaStreams = KafkaStreams(streamsBuilder.build(), properties)

        kafkaStreams.start()
    }

    private val String.tweetFollowersCount: Int
        get() = try {
            JsonParser.parseString(this)
                .asJsonObject
                .get("user")
                .asJsonObject
                .get("followers_count")
                .asInt
        } catch (e: Exception) {
            0
        }

}
