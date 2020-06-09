package kafka.tools

import java.util.{Arrays,  Properties}

import org.apache.kafka.clients.{CommonClientConfigs}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.utils.{Exit, Utils}

import scala.util.Random

object ProducerLatency {

  def main(args: Array[String]) {
    if (args.length != 5 && args.length != 6) {
      System.err.println("USAGE: java " + getClass.getName + " broker_list topic num_messages producer_acks message_size_bytes [optional] properties_file")
      Exit.exit(1)
    }

    val brokerList = args(0)
    val topic = args(1)
    val numMessages = args(2).toInt
    val producerAcks = args(3)
    val messageLen = args(4).toInt
    val propsFile = if (args.length > 5) Some(args(5)).filter(_.nonEmpty) else None
    val warmup = 2000

    if (!List("1", "all").contains(producerAcks))
      throw new IllegalArgumentException("Latency testing requires synchronous acknowledgement. Please use 1 or all")

    def loadPropsWithBootstrapServers: Properties = {
      val props = propsFile.map(Utils.loadProps).getOrElse(new Properties())
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList)
      props
    }

    val producerProps = loadPropsWithBootstrapServers
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "0") //ensure writes are synchronous
    producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MaxValue.toString)
    producerProps.put(ProducerConfig.ACKS_CONFIG, producerAcks.toString)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

    def finalise() {
      producer.close()
    }

    var totalTime = 0.0
    val latencies = new Array[Long](numMessages)
    val random = new Random(0)

    for( i <- 0 until warmup) {
      val message = randomBytesOfLen(random, messageLen)
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, message)).get()
    }

    for (i <- 0 until numMessages) {
      val message = randomBytesOfLen(random, messageLen)
      val begin = System.nanoTime
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, message)).get()
      val elapsed = System.nanoTime - begin


      //Report progress
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
    println("Producer latency: 50th = %f us, 95th = %f us, 99th = %f us, 99.9th = %f us".format(p50/ 1000.0, p95/ 1000.0, p99/ 1000.0, p999/ 1000.0))

    finalise()
  }

  def randomBytesOfLen(random: Random, len: Int): Array[Byte] = {
    Array.fill(len)((random.nextInt(26) + 65).toByte)
  }


}

