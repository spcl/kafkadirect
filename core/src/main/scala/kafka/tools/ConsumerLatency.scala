package kafka.tools

import java.time.Duration
import java.util.{Properties}

import org.apache.kafka.clients.{CommonClientConfigs}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.{Exit, Utils}

import scala.collection.JavaConverters._


object ConsumerLatency {
  private val timeout: Long = 60000
  private val entryheadersize: Int = 72 // TODO check that is always true.

  def main(args: Array[String]) {
    if (args.length != 5 && args.length != 6) {
      System.err.println("USAGE: java " + getClass.getName + " broker_list topic num_messages datasize withrdma [optional] properties_file")
      Exit.exit(1)
    }

    val brokerList = args(0)
    val topic = args(1)
    val numMessages = args(2).toInt
    val datasize = args(3).toInt
    val withrdma = args(4).contains("withrdma")
    val withrdmaslots = args(4).contentEquals("withrdmaslots")
    val propsFile = if (args.length > 5) Some(args(5)).filter(_.nonEmpty) else None

    def loadPropsWithBootstrapServers: Properties = {
      val props = propsFile.map(Utils.loadProps).getOrElse(new Properties())
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList)
      props
    }

    val consumerProps = loadPropsWithBootstrapServers
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "latency-group-" + System.currentTimeMillis())
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, (datasize + entryheadersize).toString) // for RDMA
    consumerProps.put(ConsumerConfig.WITH_SLOTS, withrdmaslots.toString) // for RDMA.
    consumerProps.put(ConsumerConfig.MEASURE_LATENCY_CONFIG, "true")
    consumerProps.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,"1");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0") //ensure we have no temporal batching
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)


    def finalise() {
      consumer.close()
    }


    val topicPartitions = consumer.partitionsFor(topic).asScala
      .map(p => new TopicPartition(p.topic(), p.partition())).asJava
    consumer.assign(topicPartitions)
    consumer.assignment().asScala.foreach(consumer.position)

    for (i <- 0 until numMessages) {
      // latency will be measured by the Fetcher of KafkaConsumer
      if(withrdma){
        consumer.RDMApoll(Duration.ofMillis(timeout))
      }else {
        consumer.poll(Duration.ofMillis(timeout))
      }
    }

    finalise()
  }

}
