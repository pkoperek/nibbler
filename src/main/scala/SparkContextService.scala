import org.apache.spark.{SparkContext, SparkConf}

class SparkContextService(nibblerJarRealPath: String) {

  private val executorUri = "http://d3kbcqa49mib13.cloudfront.net/spark-1.0.0-bin-hadoop2.tgz"
  private val defaultMasterUri = "mesos://zk://master:2181/mesos"

  def createSparkContext(appName: String): SparkContext = {
    val masterUri = System.getProperty("nibbler.master.uri", defaultMasterUri)
    createSparkContext(appName, masterUri)
  }

  def createSparkContext(appName: String, masterUri: String): SparkContext = {
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.executor.uri", executorUri)
      .setMaster(if (!masterUri.isEmpty) masterUri else defaultMasterUri)
      .setSparkHome("someHomeWhichShouldBeIrrelevant")

    val ctx = new SparkContext(conf)
    ctx.addJar(nibblerJarRealPath)
    ctx
  }
}