import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

object PipelineSample extends App {
  val spark = SparkSession
    .builder()
    .appName("PipelineSample")
    .master("local[*]")
    .config("spark.local.ip", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
  
  val trainData = spark.createDataFrame(
    Seq(
      (161.0, 69.87, 29, 1.0),
      (176.78, 74.35, 34, 1.0),
      (159.23, 58.32, 29, 0.0)
      )
    )
    .toDF("height", "weight", "age", "gender")
  
  val testData = spark.createDataFrame(
    Seq(
      (169.4, 75.3, 42),
      (185.1, 85.0, 37),
      (161.6, 61.2, 28)
      )
    )
    .toDF("height", "weight", "age") 
    
  //trainData.show(false)
  //testData.show(false)
  
  val assembler = new VectorAssembler()
    .setInputCols(Array("height", "weight", "age"))
    .setOutputCol("features")
  
  val trainAssembled = assembler.transform(trainData)
  val testAssembled = assembler.transform(testData)
  
  //trainAssembled.show(false)
  //testAssembled.show(false)
  
  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.01)
    .setLabelCol("gender")
  
  val lrModel = lr.fit(trainAssembled)
  
  //lrModel.transform(trainAssembled).show()
  //lrModel.transform(testAssembled).show()

  val pipeline = new Pipeline().setStages(Array(assembler, lr))
  
  val pipelineModel = pipeline.fit(trainData)
  
  //pipelineModel.transform(testData).show()

  val pathModel = ".\\model"
  val pathPipe = ".\\pipe"
  
  lrModel.write.overwrite().save(pathModel)
  pipelineModel.write.overwrite.save(pathPipe)
  
  val loadedModel = LogisticRegressionModel.load(pathModel)
  val loadedPipelineModel = PipelineModel.load(pathPipe)
  
  loadedModel.transform(testAssembled).show()
  loadedPipelineModel.transform(testAssembled).show()
  
  spark.stop()
}