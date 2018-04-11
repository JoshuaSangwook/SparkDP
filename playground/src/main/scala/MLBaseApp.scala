import org.apache.log4j.Logger
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import play.api.libs.json._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
// class가 낫지 않을까?
trait MLBaseApp extends App with QueryExecutionListener {

  private def appName: String = getClass.getName.split("\\$").last
  def logger: Logger = Logger.getLogger(appName)

  private def config: Config = ConfigFactory.load
  def hdfsRoot: String = config.getString("hdfs.root")
  def hiveDB: String = config.getString("hive.db")

  //{"year":"2018","month":"02","day":"03","path":"/MART/APP/mart_app_vst_ctnt","columns":["MBR_ID","DVC_ID","NEW_VST_YN"]}
  //확장가능  
  case class Argument(year: String, month: String, day: String, path: String, columns: String*)
  implicit val modelFormat = Json.format[Argument]
  
  private def argFirst: String = args(0)
  def newArgs: Argument = Json.fromJson[Argument](Json.parse(argFirst)).get

  def getSpark: SparkSession  = 
    SparkSession.builder
      .config("appName", appName)
      .config("key", "love")
      .config("spark.extraListeners", "AppEventListener")
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()
    
  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception) {
    logger.info(s"$appName $funcName failed. - " + qe.simpleString + " " + exception.printStackTrace());
  }
  //QueryExecutionListener
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long) {
    logger.info(s"$appName $funcName succeeded. It takes $durationNs " + qe.simpleString);
  }
}
