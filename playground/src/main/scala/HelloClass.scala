import org.apache.log4j.Logger

class HelloClass extends App {
  
  val gogo = args.length < 1
  val hihi = args(0)
  
  val appName = getClass.getName.split("\\$").last
  val logger = Logger.getLogger(appName)
}