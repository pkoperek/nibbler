package nibbler

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class SparkContextService(sparkContext: SparkContext) {

  private val initializedDataSets = mutable.Map()

  def registerDataSet(inputPath: String): RDD[String] = {
    sparkContext.textFile(inputPath)
  }

}

object SparkContextService {
  private val executorUri = "http://d3kbcqa49mib13.cloudfront.net/spark-1.0.0-bin-hadoop2.tgz"
  private val defaultMasterUri = "mesos://zk://master:2181/mesos"

  val nibblerMasterUriKey: String = "nibbler.master.uri"

  def apply(nibblerJarRealPath: String, appName: String): SparkContextService = {
    val masterUri = System.getProperty(nibblerMasterUriKey, defaultMasterUri)
    new SparkContextService(createSparkContext(nibblerJarRealPath, appName, masterUri))
  }

  private def createSparkContext(nibblerJarRealPath: String, appName: String, masterUri: String): SparkContext = {
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.executor.uri", executorUri)
      .setMaster(masterUri)
      .setSparkHome("someHomeWhichShouldBeIrrelevant")

    val ctx = new SparkContext(conf)
    ctx.addJar(nibblerJarRealPath)
    ctx
  }
}