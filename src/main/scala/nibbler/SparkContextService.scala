package nibbler

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.Range.Inclusive
import scala.collection.mutable

class SparkContextService(sparkContext: SparkContext) {

  private val initializedDataSets = mutable.Map[String, RDD[String]]()

  def getDataSetOrRegister(dataSetPath: String): RDD[String] = {
    val dataSet = getDataSet(dataSetPath)

    if(dataSet.isEmpty) {
      registerDataSet(dataSetPath)
    } else {
      dataSet.get
    }
  }

  def getSparkContext: SparkContext = sparkContext

  def containsDataSet(dataSetPath: String): Boolean = {
    initializedDataSets.contains(dataSetPath)
  }

  def unregisterDataSet(dataSetPath: String) = {
    initializedDataSets.remove(dataSetPath)
  }

  def getDataSet(dataSetPath: String): Option[RDD[String]] = {
    initializedDataSets.get(dataSetPath)
  }

  def registerDataSet(dataSetPath: String): RDD[String] = {
    initializedDataSets.synchronized {
      initializedDataSets.getOrElseUpdate(dataSetPath, {
        sparkContext.textFile(dataSetPath)
      })
    }
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