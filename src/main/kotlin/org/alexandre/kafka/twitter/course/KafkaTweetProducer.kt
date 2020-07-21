package org.alexandre.kafka.twitter.course

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Client
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit


class KafkaTweetProducer(
    private val twitterConsumerKey: String,
    private val twitterConsumerSecret: String,
    private val twitterToken: String,
    private val twitterTokenSecret: String,
    private val kafkaBootstrapServer: String,
    private val kafkaTopic: String,
    private val termsToTrack: List<String>
) {
    private val msgQueue = LinkedBlockingQueue<String>(MSG_QUEUE_CAPACITY)

    fun run() {
        val client = createTwitterClient(termsToTrack).also { it.connect() }
        val producer = createKafkaProducer()

        Runtime.getRuntime().addShutdownHook(
            Thread {
                logger.info("Stopping TwitterProducer application...")
                client.stop()
                producer.close()
                logger.info("TwitterProducer stopped")
            }
        )

        while (!client.isDone) {
            try {
                msgQueue.poll(POLLING_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)
                    ?.let {
                        logger.info("Sending tweet: $it")
                        producer.sendTweet(it)
                    }
            } catch (e: InterruptedException) {
                e.printStackTrace()
                client.stop()
            }
        }
    }

    private fun createTwitterClient(termsToTrack: List<String>): Client {
        val hosebirdHosts = HttpHosts(Constants.STREAM_HOST)
        val hosebirdEndpoint = StatusesFilterEndpoint().also {
            it.trackTerms(termsToTrack)
        }

        val hosebirdAuth = OAuth1(twitterConsumerKey, twitterConsumerSecret, twitterToken, twitterTokenSecret)

        return ClientBuilder()
            .name(CLIENT_NAME)
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(StringDelimitedProcessor(msgQueue))
            .build()
    }

    private fun createKafkaProducer(): KafkaProducer<String, String> {
        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer)
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        // These properties are not necessary because we are setting ENABLE_IDEMPOTENCE_CONFIG to true
        // But, since this is a POC, it is nice to have the configurations explicitly
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Int.MAX_VALUE.toString())
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")

        // High throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, KAFKA_PRODUCER_COMPRESSION_TYPE)
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, KAFKA_PRODUCER_LINGER_MS)
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, KAFKA_PRODUCER_BATCH_SIZE)


        return KafkaProducer<String, String>(properties)
    }

    private fun KafkaProducer<String, String>.sendTweet(tweet: String) {
        val record = ProducerRecord<String, String>(kafkaTopic, tweet)

        this.send(record) { _, exception ->
            if (exception != null) {
                logger.error("Error occurred while sending tweet", exception)
            }
        }

    }

    companion object {
        private const val MSG_QUEUE_CAPACITY = 100000
        private const val CLIENT_NAME = "Hosebird-Client-01"
        private const val POLLING_TIMEOUT_IN_SECONDS = 5L

        private const val KAFKA_PRODUCER_COMPRESSION_TYPE = "snappy"
        private const val KAFKA_PRODUCER_LINGER_MS = "20"
        private const val KAFKA_PRODUCER_BATCH_SIZE = (32 * 1024).toString()


        private val logger = LoggerFactory.getLogger(KafkaTweetProducer::class.java)
    }
}
