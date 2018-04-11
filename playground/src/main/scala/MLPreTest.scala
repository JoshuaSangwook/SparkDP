import org.apache.spark.sql.functions._

object MLPreTest extends MLBaseApp {
  
  val b1d_yyyy = newArgs.year
  val b1d_mm   = newArgs.month
  val b1d_dd   = newArgs.day
  val path     = newArgs.path
  val columns  = newArgs.columns.toArray.map(lit(_))  
  
  val spark = getSpark
  
  try {
    spark.listenerManager.register(this) 
    
    //테스트를 위해 의미없는 hive partition 생성
    spark.sql(
      s"""
        ALTER TABLE user_bi_ocb.wk_mart_app_bd_thm_ctnt 
        ADD IF NOT EXISTS PARTITION (base_dt='$b1d_yyyy$b1d_mm$b1d_dd') 
        LOCATION '/data_bis/ocb/MART/APP/WK/wk_mart_app_bd_thm_ctnt/$b1d_yyyy/$b1d_mm/$b1d_dd'
      """)
      
    spark.read
      .orc(s"$hdfsRoot/$path/$b1d_yyyy/$b1d_mm/$b1d_dd/poc_fg_cd=01")  
      .select(columns:_*)
      .withColumn("ld_dttm", from_unixtime(unix_timestamp(), "yyyyMMddHHmmss"))
      .withColumn("mart_upd_dttm", from_unixtime(unix_timestamp(), "yyyyMMddHHmmss"))
      .write
      .mode("overwrite")
      .orc(s"$hdfsRoot/temp/spark-sample")
      
  } catch {
    case e: Exception => {logger.error(e); throw e}
    
  } finally {
    spark.stop()
  }  
}