import org.apache.log4j.Logger
import com.typesafe.config.ConfigFactory
import play.api.libs.json._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution

class MLBase extends App with QueryExecutionListener {

  private val appName = getClass.getName.split("\\$").last
  protected val logger = Logger.getLogger(appName)

  private val config = ConfigFactory.load
  protected val hdfsRoot = config.getString("hdfs.root")
  protected val hiveDB = config.getString("hive.db")
 
  case class Argument(
      anything: Option[String] //= Some("")
    , feature:  Option[String] //= Some("")  
    , year:     Option[String] //= Some("")
    , month:    Option[String] //= Some("")
    , day:      Option[String] //= Some("")
    , path:     Option[String] //= Some("")
    , columns:  Option[List[String]] //= Some(Nil)
  )
  implicit val modelFormat = Json.using[Json.WithDefaultValues].format[Argument]
  protected val newArgs = Json.fromJson[Argument](Json.parse(args(0))).get

  protected val spark  = 
    SparkSession.builder
      .config("appName", appName)
      .config("key", "love")
      .config("spark.extraListeners", "AppEventListener")
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()
      
  spark.listenerManager.register(this) 
  
  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception) {
    logger.info(s"$appName $funcName failed. - " + qe.simpleString + " " + exception.printStackTrace());
  }
  //QueryExecutionListener
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long) {
    logger.info(s"$appName $funcName succeeded. It takes $durationNs " + qe.simpleString);
  }
}