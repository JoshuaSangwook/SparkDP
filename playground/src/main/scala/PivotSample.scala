import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object PivotSample extends App {
  val spark = SparkSession
    .builder()
    .appName("PivotSample")
    .master("local[*]")
    .getOrCreate()
    
  val df = spark.createDataFrame(
    Seq(
        ("서울", 9904312, 7904312)
      , ("인천", 9463322, 7463352)
      , ("부산", 9345682, 7345642)
      , ("광주", 9534392, 7534332)
      )
    ).toDF("city", "2010", "2015")
    
   df.show()

   import df.sparkSession.implicits._ // For toDF RDD -> DataFrame conversion
   val schema = df.schema
   
   val df2 = df.flatMap(row => {
     val metric = row.getString(0)
     (1 until row.size).map(i => (metric, schema(i).name, row.getInt(i)))
     }
   )
   .toDF("city", "year", "pop")
   
   df2.show()
   
   val df3 = df2.groupBy("city").pivot("year").agg(first("pop"))
   df3.show()
   
  spark.stop()
}