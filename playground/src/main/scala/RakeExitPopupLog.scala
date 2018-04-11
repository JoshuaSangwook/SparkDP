import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._


object RakeExitPopupLog extends App {
  
  val conf = new SparkConf().setAppName("RakeExitPopupLog")
  val sc = new SparkContext(conf);
  val ssc = new StreamingContext(sc, Seconds(3))
  
  val brokerList = "DICc-broker01.cm.skp:9092,DICc-broker02.cm.skp:9092,DICc-broker03.cm.skp:9092,DICc-broker04.cm.skp:9092,DICc-broker05.cm.skp:9092"
  val ds = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, Map("metadata.broker.list" -> brokerList), Set("ocb-tc-discover-log-01"))

  //ds.map((_, 1)).print
  //ds.flatMap(_._2.split(",")).print
  //ds.mapValues(_.split(",").length).reduceByKey(_ + _).print
  ds.print()
  
  ssc.start
  ssc.awaitTermination()  
}