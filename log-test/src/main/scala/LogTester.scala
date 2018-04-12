import org.apache.log4j.Logger

object LogTester extends App {

  val app = Logger.getLogger("console")
  app.info("hello, world")

  val file = Logger.getLogger("file")
  file.info("just for test")

  val kafka = Logger.getLogger("kafka")
  kafka.info("fly me to the kafka")
}