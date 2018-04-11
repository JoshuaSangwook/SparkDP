import org.apache.spark.sql.functions._

object MLPreSample extends MLBase {
  
  val b1d_yyyy = newArgs.year.getOrElse("")   //get
  val b1d_mm   = newArgs.month.getOrElse("")  //get
  val b1d_dd   = newArgs.day.getOrElse("")    //get
  val path     = newArgs.path.getOrElse("")   //get
  val columns  = newArgs.columns.getOrElse(Nil).map(col(_))  //get 

  try {
    // 주요 argument 체크
    if (path == "") throw new IllegalArgumentException("Not enough arguments." + newArgs.toString())
   
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