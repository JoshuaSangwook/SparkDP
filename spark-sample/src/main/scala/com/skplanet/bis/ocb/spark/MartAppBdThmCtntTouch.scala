package com.skplanet.bis.ocb.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.log4j.LogManager
import java.security.MessageDigest
import java.util
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import scala.util.parsing.json._
import com.di.security.CryptoUtils

object MartAppBdThmCtntTouch extends App {
  
  val appName = this.getClass.getName.split("\\$").last
  
  val logger = org.apache.log4j.LogManager.getLogger(appName)
  
  if(args.length < 3) {
    logger.error(s"Usage: $appName yyyy MM dd")
    throw new IllegalArgumentException("Not enough arguments. " + args.mkString(" "))
  }
  
  val decryptAesUDF = udf(CryptoUtils.decryptAes(_: String, _:String))
  
  val getJsonValue = udf((jStr: String, key: String) => {
    try {  
      // JSON 모듈버전
      JSON.parseFull(jStr).get.asInstanceOf[Map[String, String]](key)
      /*
      // 단순파싱 버전
      val jsMap = jStr.substring(1, jStr.length - 1)
        .split(",")
        .map(_.split(":"))
        .map {case Array(k, v) => (k.substring(1, k.length-1), v.substring(1, v.length-1))}
        .toMap
      jsMap(key) 
      */      
    } catch {
      case e: Exception => ""
	  }
  })
  
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
    spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", "\t")
      .csv(s"hdfs://skpds/data/ocb/tsv/touch-v5/$b1d_yyyy/$b1d_mm/$b1d_dd/*/*")
      .select(
          col("_c3").as("dvc_id")
        , col("_c6").as("session_id") 
        , col("_c26").as("page_id") 
        , col("_c27").as("actn_id") 
        , col("_c28").as("body")
      )
      //.na.fill(Map("page_id"->"UNKNOWN")) 
      .na.fill(Map("actn_id"->"UNKNOWN")) 
      //.filter(!upper(col("actn_id")).isin("EVENT_RECEIVE", "SERVER_COMM"))   

      .filter(lower(trim(col("page_id"))).isin("/main", "/intro"))
      .filter(col("actn_id") === "UNKNOWN")  

      .withColumn("mbr_id", decryptAesUDF(translate(getJsonValue(col("body"), lit("mbr_id")),"\n\r\t",""), lit("38880189683628965483845997893130223408")))
      .withColumn("common_cd", translate(getJsonValue(col("body"), lit("common_code")),"\n\r\t",""))
      
      .select(
          col("mbr_id")
        , when(length(split(col("session_id"), "_").getItem(1)) > 0, split(col("session_id"), "_").getItem(1)).otherwise(col("dvc_id")).as("vstr_id")
        , col("page_id")
        , split(col("common_cd"), "_").getItem(0).as("common_cd")
        , col("actn_id") 
      )
      .write
      .mode("overwrite")
      .orc("hdfs://skpds/user/pp23583/spark")

        
    val dfLog =
      spark.read
        .orc("hdfs://skpds/user/pp23583/spark")
        .select(
            col("mbr_id")
          , col("vstr_id") 
          , col("page_id") 
          , col("common_cd") 
          , col("actn_id")
        )
        
    //hive partition 생성
    spark.sql(
      s"""
        ALTER TABLE user_bi_ocb.wk_mart_app_bd_thm_ctnt 
        ADD IF NOT EXISTS PARTITION (base_dt='$b1d_yyyy$b1d_mm$b1d_dd') 
        LOCATION '/data_bis/ocb/MART/APP/WK/wk_mart_app_bd_thm_ctnt/$b1d_yyyy/$b1d_mm/$b1d_dd'
      """)

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
    dfTheme
      .join(dfLog, dfTheme("seq") === dfLog("common_cd"), "inner")
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
.filter(col("mbr_id") === "313390661")  
.take(350).foreach(logger.info) 

.filter(col("mbr_id") === "313390661")  
.write
.mode("overwrite")
.csv("hdfs://skpds/user/pp23583/spark")
*/