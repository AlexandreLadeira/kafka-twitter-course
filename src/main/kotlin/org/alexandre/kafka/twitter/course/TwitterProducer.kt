package org.alexandre.kafka.twitter.course

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Client
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.OAuth1
import org.slf4j.LoggerFactory
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit


class TwitterProducer(
    private val consumerKey: String,
    private val consumerSecret: String,
    private val token: String,
    private val tokenSecret: String
) {
    private val msgQueue = LinkedBlockingQueue<String>(MSG_QUEUE_CAPACITY)

    fun run() {
        val client = createTwitterClient(listOf("Aurora")).also { it.connect() }

        while (!client.isDone) {
            try {
                msgQueue.poll(POLLING_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)
                    ?.let { logger.info("Message: $it") }
            } catch (e: InterruptedException) {
                e.printStackTrace()
                client.stop()
            }
        }

        logger.trace("Shutting down TwitterProducer")
        client.stop()
        logger.info("TwitterProducer stopped")
    }

    private fun createTwitterClient(termsToTrack: List<String>): Client {
        val hosebirdHosts = HttpHosts(Constants.STREAM_HOST)
        val hosebirdEndpoint = StatusesFilterEndpoint().also {
            it.trackTerms(termsToTrack)
        }

        val hosebirdAuth = OAuth1(consumerKey, consumerSecret, token, tokenSecret)

        return ClientBuilder()
            .name(CLIENT_NAME)
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(StringDelimitedProcessor(msgQueue))
            .build()
    }

    companion object {
        private const val MSG_QUEUE_CAPACITY = 100000
        private const val CLIENT_NAME = "Hosebird-Client-01"
        private const val POLLING_TIMEOUT_IN_SECONDS = 5L

        private val logger = LoggerFactory.getLogger(TwitterProducer::class.java)
    }
}
