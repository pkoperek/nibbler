package nibbler

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.Range.Inclusive
import scala.collection.mutable

class SparkContextService(sparkContext: SparkContext) {

  private val initializedDataSets = mutable.Map[String, DataSet]()

  def getDataSetOrRegister(dataSetPath: String): DataSet = {
    val dataSet = getDataSet(dataSetPath)

    if (dataSet.isEmpty) {
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

  def getDataSet(dataSetPath: String): Option[DataSet] = {
    initializedDataSets.get(dataSetPath)
  }

  def registerDataSet(dataSetPath: String): DataSet = {
    initializedDataSets.synchronized {
      initializedDataSets.getOrElseUpdate(dataSetPath, {
        val rdd = sparkContext.textFile(dataSetPath)

        val rowsCount = rdd.count()
        val columnsCount = if (rowsCount > 0) rdd.first().split(",").length else 0

        new DataSet(
          rowsCount,
          columnsCount,
          rdd
        )
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