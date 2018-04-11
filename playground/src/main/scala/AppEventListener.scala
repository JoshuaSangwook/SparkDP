import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationStart, SparkListenerApplicationEnd}
import org.apache.log4j.Logger

class AppEventListener(conf: SparkConf) extends SparkListener {
  
  val appName = conf.get("appName")
  val logger = Logger.getLogger(appName)
  val key = conf.get("key")
  
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    logger.info(s"Spark Application($appName $key) Start: " + applicationStart.appName);
  }
  
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    logger.info(s"Spark Application($appName $key) End: " + applicationEnd.time);
  }  
}