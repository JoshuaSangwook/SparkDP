package com.skplanet.bis.ocb.spark

import com.typesafe.config.ConfigFactory
//import net.liftweb.json._
import play.api.libs.json._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.execution.QueryExecution
import org.apache.log4j.Logger

object MartAppBdThmCtntNew extends App with QueryExecutionListener {
  
  val appName = this.getClass.getName.split("\\$").last
  val logger = Logger.getLogger(appName)

  if(args.length < 1) {
    throw new IllegalArgumentException("Not enough arguments. " + args.mkString(" "))
  }

  val config = ConfigFactory.load
  val hdfsRoot = config.getString("hdfs.root")
  val hiveDB = config.getString("hive.db")

    //{"year":"2018","month":"02","day":"03","path":"/MART/APP/mart_app_vst_ctnt","columns":["MBR_ID","VSTR_ID"]}
  //implicit val formats = DefaultFormats //lift-json
  case class Inquiry(year: String, month: String, day: String, path: String, columns: String*)
  implicit val modelFormat = Json.format[Inquiry]
  
  //val inq = jsonArgs.extract[Inquiry] //lift-json
  val inq = Json.fromJson[Inquiry](Json.parse(args(0))).get
  
  val b1d_yyyy = inq.year
  val b1d_mm   = inq.month
  val b1d_dd   = inq.day
  val path     = inq.path
  val columns  = inq.columns.toList.map(col(_))
  
  val spark = 
    SparkSession.builder
      .config("appName", appName)
      .config("key", "love")
      .config("spark.extraListeners", "com.skplanet.bis.ocb.spark.AppEventListener")
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()

  spark.listenerManager.register(this)     
  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception) {
    logger.info(s"$funcName failed. - " + exception.printStackTrace());
  }
  //QueryExecutionListener
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long) {
    logger.info(s"$funcName succeeded. It takes $durationNs");
  }
  
  try {
    
    //테스트를 위해 의미없는 hive partition 생성
    spark.sql(
      s"""
        ALTER TABLE user_bi_ocb.wk_mart_app_bd_thm_ctnt 
        ADD IF NOT EXISTS PARTITION (base_dt='$b1d_yyyy$b1d_mm$b1d_dd') 
        LOCATION '/data_bis/ocb/MART/APP/WK/wk_mart_app_bd_thm_ctnt/$b1d_yyyy/$b1d_mm/$b1d_dd'
      """)
      
    spark.read
      .orc("hdfs://skpds/user/pp23583/spark")  
      .select(columns:_*)
      .withColumn("ld_dttm", from_unixtime(unix_timestamp(), "yyyyMMddHHmmss"))
      .withColumn("mart_upd_dttm", from_unixtime(unix_timestamp(), "yyyyMMddHHmmss"))
      .write
      .mode("overwrite")
      .csv(s"$hdfsRoot/temp/spark-sample")
      
  } catch {
    case e: Exception => {logger.error(e); throw e}
    
  } finally {
    spark.stop()
  }
}