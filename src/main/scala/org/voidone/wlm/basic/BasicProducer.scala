package org.voidone.wlm.basic



import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object BasicProducer {

  def main(args: Array[String]): Unit = {

    // Config properties
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "wlm-basic-message-producer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    // Create Producer
    val producer = new KafkaProducer[String, String](props)

    // Producer Record
    val producerRecord = new ProducerRecord[String, String]("first-topic", "Hello Neo, ")

    // Send Data
    producer.send(producerRecord)

    // Flush and close
    producer.flush()
    producer.close()

  }

}
