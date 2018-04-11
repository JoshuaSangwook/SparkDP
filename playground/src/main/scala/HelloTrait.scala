import org.apache.log4j.Logger

trait HelloTrait extends App {
  
  def gogo: Boolean = args.length < 1
  def hihi: String = args(0)
  
  val appName = getClass.getName.split("\\$").last
  val logger = Logger.getLogger(appName)
  
}