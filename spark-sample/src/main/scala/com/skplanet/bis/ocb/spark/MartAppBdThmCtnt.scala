package com.skplanet.bis.ocb.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object MartAppBdThmCtnt extends App {
    
  val appName = this.getClass.getName.split("\\$").last
  
  val logger = org.apache.log4j.LogManager.getLogger(appName)
  
  if(args.length < 3) {
    logger.error(s"Usage: $appName yyyy MM dd")
    throw new IllegalArgumentException("Not enough arguments. " + args.mkString(" "))
  }
  
  val b1d_yyyy = args(0)
  val b1d_mm = args(1)
  val b1d_dd = args(2)

  val spark = 
    SparkSession.builder
      .appName(appName)
      //.config("spark.sql.warehouse.dir", "hdfs://skpds/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
        
  try {
    //hive partition 생성
    spark.sql(
      s"""
        ALTER TABLE user_bi_ocb.wk_mart_app_bd_thm_ctnt 
        ADD IF NOT EXISTS PARTITION (base_dt='$b1d_yyyy$b1d_mm$b1d_dd') 
        LOCATION '/data_bis/ocb/MART/APP/WK/wk_mart_app_bd_thm_ctnt/$b1d_yyyy/$b1d_mm/$b1d_dd'
      """)
   
    //통합로그 로딩...
    val dfLog = 
      spark.read
        .option("header", "false")
        .option("inferSchema", "false")
        .option("delimiter", "\t")
        .csv(s"hdfs://skpds/product/BIS_SERVICES/OCB/INTG/OCB_D_INTG_LOG/poc=01/dt=$b1d_yyyy$b1d_mm$b1d_dd")
        .select(
            col("_c1").as("vstr_id")
          , col("_c5").as("mbr_id")
          , col("_c28").as("page_id")
          , col("_c29").as("actn_id")
          , split(col("_c63"), "_").getItem(0).as("common_cd")
        )
        .filter(lower(trim(col("page_id"))).isin("/main", "/intro"))
        .filter(lower(trim(col("actn_id"))) === "unknown")  
        .na.replace("mbr_id", Map("" -> "", "\\N" -> null)) // hive에서 null을 \\N 텍스트로 입력해 놓음. 이 spark functions의 결과는 추후 hive에서 조회하면 null은 없고 ''공백문자임.
        //.na.fill(Map("mbr_id"->"")) // na.fill이 잡아내는 것은 hdfs의 text파일의 ""공백임
    
    //브렌드데이 테마 meta데이터 로딩...
    val dfTheme = 
      spark.sql(
        """
          SELECT seq
               , title
            FROM OCB.DBM_SOI_TC_APP_THEME_V
           WHERE lower(trim(dbsts)) = 'o'
        """)
    
    //로그와 테마 join 후 출력
    dfTheme.join(dfLog, dfTheme("seq") === dfLog("common_cd"), "inner")
      .select(
          dfLog("mbr_id")
        , dfLog("vstr_id").as("dvc_id")
        , dfTheme("title")
        , when(lower(trim(dfLog("page_id"))) === "/intro", "Y").otherwise("N").as("ldng_them_vst_yn")
        , when(lower(trim(dfLog("page_id"))) === "/main", "Y").otherwise("N").as("main_them_vst_yn")
        , from_unixtime(unix_timestamp(), "yyyyMMddHHmmss").as("ld_dttm")
        , from_unixtime(unix_timestamp(), "yyyyMMddHHmmss").as("mart_upd_dttm")
        , dfTheme("seq")
      )
      .write
      .mode("overwrite")
      .orc(s"hdfs://skpds/data_bis/ocb/MART/APP/WK/wk_mart_app_bd_thm_ctnt/$b1d_yyyy/$b1d_mm/$b1d_dd")
      
  } catch {
    case e: Exception => {logger.error(e); throw e}
    
  } finally {
    spark.stop()
  }
}

/*
object MartAppBdThmCtnt extends App {
  
  if(args.length < 2)
    System.exit(1)
    
  val b1d_yyyy = args(0)
  val b1d_mm = args(1)
  val b1d_dd = args(2)
  
  val spark = SparkSession
    .builder
    .appName(MartAppBdThmCtnt.getClass.getName.split("\\$").last)
    //.config("spark.sql.warehouse.dir", "hdfs://skpds/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()

  //hive partition 생성
  spark
    .sql(s"""
      ALTER TABLE user_bi_ocb.wk_mart_app_bd_thm_ctnt 
        ADD IF NOT EXISTS PARTITION (base_dt='$b1d_yyyy$b1d_mm$b1d_dd') 
        LOCATION '/data_bis/ocb/MART/APP/WK/wk_mart_app_bd_thm_ctnt/$b1d_yyyy/$b1d_mm/$b1d_dd'
      """
    )
    
  import spark.implicits._
    
  val dfLog = spark
    .read
    .option("header", "false")
    .option("inferSchema", "false")
    .option("delimiter", "\t")
    .csv(s"hdfs://skpds/product/BIS_SERVICES/OCB/INTG/OCB_D_INTG_LOG/poc=01/dt=$b1d_yyyy$b1d_mm$b1d_dd")
    .select(
        $"_c1".as("vstr_id")
      , $"_c5".as("mbr_id")
      , $"_c28".as("page_id")
      , $"_c29".as("actn_id")
      , split($"_c63", "_").getItem(0).as("common_cd")
    )
    .filter(lower(trim($"page_id")).isin("/main", "/intro"))
    .filter(lower(trim($"actn_id")) === "unknown")  
    .na.replace("mbr_id", Map("" -> "", "\\N" -> null)) 
    // hive에서 null을 \\N 텍스트로 입력해 놓음. 이결과는 모두 hive에서 조회하면 ''공백문자임.
    // na.fill(Map("mbr_id"->"")) => na.fill가 잡아내는 것은 hdfs의 text파일의 ""공백임
  
  val dfTheme = spark
    .sql(
      """
        SELECT seq
             , title
          FROM OCB.DBM_SOI_TC_APP_THEME_V
         WHERE lower(trim(dbsts)) = 'o'
      """
    )
    
  dfTheme.alias("T")
    .join(dfLog.alias("L"), $"T.seq" === $"L.common_cd", "inner")
    .select(
        $"L.mbr_id"
      , $"L.vstr_id".as("dvc_id")
      , $"T.title"
      , when(lower(trim($"L.page_id")) === "/intro", "Y").otherwise("N").as("ldng_them_vst_yn")
      , when(lower(trim($"L.page_id")) === "/main", "Y").otherwise("N").as("main_them_vst_yn")
      , from_unixtime(unix_timestamp(), "yyyyMMddHHmmss").as("ld_dttm")
      , from_unixtime(unix_timestamp(), "yyyyMMddHHmmss").as("mart_upd_dttm")
      , $"T.seq"
    )
    .write
    .mode("overwrite")
    .orc(s"hdfs://skpds/data_bis/ocb/MART/APP/WK/wk_mart_app_bd_thm_ctnt/$b1d_yyyy/$b1d_mm/$b1d_dd")

  spark.stop()
}
*/