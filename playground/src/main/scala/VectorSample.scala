import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

object VectorSample extends App {
  val v1 = Vectors.dense(0.1, 0.0, 0.2, 0.3)
  val v2 = Vectors.dense(Array(0.1, 0.0, 0.2, 0.3))
  val v3 = Vectors.sparse(4, Seq((0,0.1),(2,0.2),(3,0.3)))
  val v4 = Vectors.sparse(4, Array(0,2,3), Array(0.1,0.2,0.3))
  
  val v5 = LabeledPoint(1.0, v1)
  
  println(v1.toArray.mkString(", "))
  println(v2.toArray.mkString(", "))
  println(v3.toArray.mkString(", "))
  println(v4.toArray.mkString(", "))
  
  println(s"label:${v5.label} feature:${v5.features}")
  
  val spark  = 
    SparkSession.builder
      .appName("VectorSample")
      .master("local[*]")
      .getOrCreate()
      
  val path = "C:\\Apps\\spark\\data\\mllib\\sample_lda_libsvm_data.txt"
  val v6 = MLUtils.loadLibSVMFile(spark.sparkContext, path)
  val lp1 = v6.first
  println(s"label:${lp1.label} feature:${lp1.features}")
  
  spark.stop
}