/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.{Arrays, Collections, Properties}

import kafka.utils.{CommandLineUtils}
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.{CommonClientConfigs, admin}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._
import scala.util.Random


/**
 * This class records the average end to end latency for a single message to travel through Kafka
 *
 * broker_list = location of the bootstrap broker for both the producer and the consumer
 * num_messages = # messages to send
 * producer_acks = See ProducerConfig.ACKS_DOC
 * message_size_bytes = size of each message in bytes
 *
 * e.g. [localhost:9092 test 10000 1 20]
 */

object EndToEndLatency {
  private val timeout: Long = 60000
  private val defaultReplicationFactor: Short = 1
  private val defaultNumPartitions: Int = 1
  private val entryheadersize: Int = 70 // TODO check that is always true.

  def main(args: Array[String]) {

    val config = new TestConfig(args)

    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config.consumerProps)
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](config.producerProps)

    val topic =  config.topic
    val messageLen = config.messageLen
    val numMessages = config.numMessages
    val warmup = 2000

    def finalise() {
      consumer.commitSync()
      producer.close()
      consumer.close()
    }

    // create topic if it does not exist
    if (!consumer.listTopics().containsKey(topic)) {
      try {
        createTopic(topic, config.createTopicProps)
      } catch {
        case t: Throwable =>
          finalise()
          throw new RuntimeException(s"Failed to create topic $topic", t)
      }
    }

    val topicPartitions = consumer.partitionsFor(topic).asScala
      .map(p => new TopicPartition(p.topic(), p.partition())).asJava
    consumer.assign(topicPartitions)
    consumer.seekToEnd(topicPartitions)
    consumer.assignment().asScala.foreach(consumer.position)

    var totalTime = 0.0
    val latencies = new Array[Double](numMessages)
    val random = new Random(0)

    for( i <- 0 until warmup) {
      val message = randomBytesOfLen(random, messageLen)
      if (config.withRdmaProduce){
        producer.RDMAsend(new ProducerRecord[Array[Byte], Array[Byte]](topic, message)).get()
      }else{
        producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, message)).get()
      }
      val recordIter =  if (config.withRdmaConsume){
              consumer.RDMApoll(Duration.ofMillis(timeout)).iterator
            }else{
              consumer.poll(Duration.ofMillis(timeout)).iterator
            }
      // Check we got results
      if (!recordIter.hasNext) {
        finalise()
        throw new RuntimeException(s"poll() timed out before finding a result (timeout:[$timeout])")
      }
    }


    println("Iteration \t Latency, us")
    for (i <- 0 until numMessages) {
      val message = randomBytesOfLen(random, messageLen)
      val begin = System.nanoTime

      // Send message (of random bytes) synchronously then immediately poll for it
      if (config.withRdmaProduce){
        producer.RDMAsend(new ProducerRecord[Array[Byte], Array[Byte]](topic, message)).get()
      }else{
        producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, message)).get()
      }

      val recordIter = if (config.withRdmaConsume)
                            consumer.RDMApoll(Duration.ofMillis(timeout)).iterator
                       else
                            consumer.poll(Duration.ofMillis(timeout)).iterator

      val elapsed = System.nanoTime - begin

      // Check we got results
      if (!recordIter.hasNext) {
        finalise()
        throw new RuntimeException(s"poll() timed out before finding a result (timeout:[$timeout])")
      }

      // Check result matches the original record
      val sent = new String(message, StandardCharsets.UTF_8)
      val read = new String(recordIter.next().value(), StandardCharsets.UTF_8)
      if (!read.equals(sent)) {
        finalise()
        throw new RuntimeException(s"The message read [$read] did not match the message sent [$sent]")
      }

      // Check we only got the one message
      if (recordIter.hasNext) {
        val count = 1 + recordIter.asScala.size
        throw new RuntimeException(s"Only one result was expected during this test. We found [$count]")
      }

      if (i % 1000 == 0)
        println(i + "\t" + elapsed / 1000.0 + " us")
      totalTime += elapsed
      latencies(i) = elapsed
    }

    //Results
    println("Avg latency: %.4f us\n".format(totalTime / numMessages / 1000.0 ))
    Arrays.sort(latencies)
    val p50 = latencies((latencies.length * 0.5).toInt)
    val p95 = latencies((latencies.length * 0.95).toInt)
    val p99 = latencies((latencies.length * 0.99).toInt)
    val p999 = latencies((latencies.length * 0.999).toInt)
    println("EndToEnd latency: 50th = %f us, 95th = %f us, 99th = %f us, 99.9th = %f us".format(p50/ 1000.0, p95/ 1000.0, p99/ 1000.0, p999/ 1000.0))

    finalise()
  }

  def randomBytesOfLen(random: Random, len: Int): Array[Byte] = {
    Array.fill(len)((random.nextInt(26) + 65).toByte)
  }

  def createTopic(topic: String, props: Properties): Unit = {
    println("Topic \"%s\" does not exist. Will create topic with %d partition(s) and replication factor = %d"
              .format(topic, defaultNumPartitions, defaultReplicationFactor))

    val adminClient = admin.AdminClient.create(props)
    val newTopic = new NewTopic(topic, defaultNumPartitions, defaultReplicationFactor)
    try adminClient.createTopics(Collections.singleton(newTopic)).all().get()
    finally Utils.closeQuietly(adminClient, "AdminClient")
  }

  import kafka.utils.CommandDefaultOptions

  class TestConfig(args: Array[String]) extends CommandDefaultOptions(args) {

    val bootstrapServersOpt = parser.accepts("broker-list", "REQUIRED: The server(s) to connect to.")
      .withRequiredArg()
      .describedAs("host")
      .ofType(classOf[String])

    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])

    val numMessagesOpt = parser.accepts("messages", "REQUIRED: The number of messages to send or consume")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Integer])

    val acksOpt = parser.accepts("producer_acks", "REQUIRED: how many acks to wait.")
      .withRequiredArg
      .describedAs("acks")
      .ofType(classOf[String])

    val fetchSizeOpt = parser.accepts("size", "The amount of data to produce/fetch in a single test.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(100)

    val withRdmaConsumeOpt = parser.accepts("with-rdma-consume", "use rdma for fetching.")
    val withRdmaProduceOpt = parser.accepts("with-rdma-produce", "use rdma for producing.")

    val withSlotsOpt = parser.accepts("withslots", "use rdma slots for fetching.")

    val consumerConfigOpt = parser.accepts("consumer.config", "Consumer config properties file.")
      .withRequiredArg
      .describedAs("consumer config file")
      .ofType(classOf[String])

    val producerConfigOpt = parser.accepts("producer.config", "Producer config properties file.")
      .withRequiredArg
      .describedAs("producer config file")
      .ofType(classOf[String])


    options = parser.parse(args: _*)

    CommandLineUtils.printHelpAndExitIfNeeded(this, "This tool helps in end to end latency")

    CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, numMessagesOpt, bootstrapServersOpt,acksOpt,fetchSizeOpt)


    val withRdmaConsume = options.has(withRdmaConsumeOpt)
    val withRdmaProduce = options.has(withRdmaProduceOpt)

    val withrdmaslots = options.has(withSlotsOpt) //extremely important


    val consumerProps = if (options.has(consumerConfigOpt))
      Utils.loadProps(options.valueOf(consumerConfigOpt))
    else
      new Properties

    val acks = options.valueOf(acksOpt)
    val topic = options.valueOf(topicOpt)
    val numMessages = options.valueOf(numMessagesOpt).intValue
    val messageLen = options.valueOf(fetchSizeOpt).intValue



    consumerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServersOpt))
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis())
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    consumerProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, (messageLen + entryheadersize).toString) // for RDMA
    consumerProps.put(ConsumerConfig.WITH_SLOTS, withrdmaslots.toString) // for RDMA
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0") // ensure we have no temporal batching


    val createTopicProps =  new Properties
    createTopicProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServersOpt))



    val producerProps = if (options.has(producerConfigOpt))
      Utils.loadProps(options.valueOf(producerConfigOpt))
    else
      new Properties

    producerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServersOpt))
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "0") // ensure writes are synchronous
    producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MaxValue.toString)
    producerProps.put(ProducerConfig.ACKS_CONFIG, options.valueOf(acksOpt))
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")



  }
}
