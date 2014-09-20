package nibbler

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class SparkContextService(sparkContext: SparkContext) extends Serializable {

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

class NibblerRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[DataSet])
    kryo.register(classOf[ErrorCalculationFunction])
    kryo.register(classOf[Function])
    kryo.register(classOf[FunctionErrorEvaluator])
    kryo.register(classOf[FunctionNode])
    kryo.register(classOf[HistdataInputParser])
    kryo.register(classOf[HistdataTimestampParser])
    kryo.register(classOf[NumericalDifferentiator])
    kryo.register(classOf[PairGenerator])
    kryo.register(BasicFunctions.getClass)
  }
}

object SparkContextService {
  private val executorUri = "http://d3kbcqa49mib13.cloudfront.net/spark-1.1.0-bin-hadoop2.4.tgz"
  private val defaultMasterUri = "mesos://zk://master:2181/mesos"
  private val defaultExecutorMemory = "512m"

  val nibblerMasterUriKey: String = "nibbler.master.uri"
  val sparkExecutorMemory: String = "spark.executor.memory"

  def apply(nibblerJarRealPath: String, appName: String): SparkContextService = {
    new SparkContextService(createSparkContext(nibblerJarRealPath, appName))
  }

  private def createSparkContext(nibblerJarRealPath: String, appName: String): SparkContext = {
    val masterUri = System.getProperty(nibblerMasterUriKey, defaultMasterUri)
    val executorMemory = System.getProperty(sparkExecutorMemory, defaultExecutorMemory)

    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.executor.uri", executorUri)
      .setMaster(masterUri)
      .set("spark.executor.memory", executorMemory)
      .setSparkHome("someHomeWhichShouldBeIrrelevant")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "nibbler.NibblerRegistrator")

    val ctx = new SparkContext(conf)
    ctx.addJar(nibblerJarRealPath)
    ctx
  }
}