object HelloWorld extends HelloClass {
  if (gogo) {
    throw new IllegalArgumentException("Not enough arguments. " + args.mkString(" "))
  }
  println(s"wow $hihi $appName")
  logger.info(s"again $hihi $appName")
}