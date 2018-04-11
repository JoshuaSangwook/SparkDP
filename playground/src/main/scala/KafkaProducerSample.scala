import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random
import java.text.SimpleDateFormat

object KafkaProducerSample extends App {
  val repCnt = args(0).toInt //반복횟수
  val topic = args(1)
  val brokers = args(2)
  
  val props = new Properties()
  props.put("bootstrap.servers", brokers)

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "1")
  props.put("producer.type", "sync")
  
  val producer = new KafkaProducer[String, String](props)

  try {
    val rnd = new Random()
    val fakePage = Set("/main", "/intro", "/detail", "unknown")
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    
    for (n <- Range(0, repCnt)) {
      val key = format.format(new Date()) 
      val value = "00000" + rnd.alphanumeric.filter(_.isDigit).take(5).mkString + 
        "," + rnd.alphanumeric.take(10).mkString + 
        "," + fakePage.toVector(rnd.nextInt(fakePage.size))
        
      val data = new ProducerRecord[String, String](topic, key, value)
  
      producer.send(data)
      Thread.sleep(500)
    }
    
  } catch {
    case e: Exception => {println(e); throw e}
    
  } finally {
    producer.close()
  }
}