import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.File

case class Person(name: String, age: Int = 0, job: String = "")

object DataSetTest {
  def main(args:Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("basic")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
    
    import spark.implicits._
    
    val srcDir = "data"
    val ds = spark
      .read
      .textFile(srcDir + File.separatorChar + "person.csv")
      .map(_.split(","))
      .map{ case arr if arr.length == 1 => Person(arr(0))
            case arr if arr.length == 2 => Person(arr(0), toInt(arr(1)))
            case arr if arr.length == 3 => Person(arr(0), toInt(arr(1)), arr(2)) }
      .persist()
    
    ds.show()
    //ds.groupByKey(_.job).count().show(false)
    //ds.groupByKey(_.job).agg(max("age").as[Int], countDistinct("age").as[Long]).show()
    //ds.groupByKey(_.job).mapValues(p => p.name + "(" + p.age + ")").reduceGroups((s1, s2) => s1 + s2).show()
   
  
    ds.write.parquet(srcDir + File.separatorChar + "person")
    val df = spark
      .read
      .load(srcDir + File.separatorChar + "person")
    
    df.show()
   
  }
  
  def toInt(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => 0
    }
  }
}