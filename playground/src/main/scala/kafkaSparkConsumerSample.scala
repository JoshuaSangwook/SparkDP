import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._

object kafkaSparkConsumerSample extends App {
  val conf = new SparkConf().setMaster("local[3]").setAppName("kafkaSparkConsumerSample")
  val sc = new SparkContext(conf);
  val ssc = new StreamingContext(sc, Seconds(3))
  val zkQuorum = "localhost:2181"
  val groupId = "test-consumer-group1"
  val topics = Map("test" -> 3)
  
  //val ds = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)
  val ds = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, Map("metadata.broker.list" -> "localhost:9092"), Set("test"))

  //ds.map((_, 1)).print
  //ds.flatMap(_._2.split(",")).print
  ds.mapValues(_.split(",").length).reduceByKey(_ + _).print
  
  ssc.start
  ssc.awaitTermination()
  
}

/*
 * 싱행시
 * spark-streaming-kafka-0-8_2.11-2.2.1.jar
 * kafka_2.11-0.8.2.1.jar
 * metrics-core-2.2.0.jar
 * zkclient-0.3.jar
 * kafka-clients-0.8.2.1.jar
 * 를 spark lib 경로에 넣어주어야 함;;;;
 */
