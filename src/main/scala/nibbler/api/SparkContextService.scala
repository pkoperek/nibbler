package nibbler.api

import com.esotericsoftware.kryo.Kryo
import nibbler.evaluation._
import nibbler.io.{HistdataInputParser, HistdataTimestampParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class SparkContextService(sparkContext: SparkContext) extends Serializable {

  private val initializedDataSets = mutable.Map[String, DataSet]()
  private val pairGenerator = new PairGenerator

  def getDataSetOrRegister(dataSetPath: String): DataSet = {
    getDataSetOrRegister(dataSetPath, "backward")
  }

  def getDataSetOrRegister(dataSetPath: String, differentiatorType: String): DataSet = {
    val dataSet = getDataSet(dataSetPath)

    if (dataSet.isEmpty) {
      registerDataSet(dataSetPath, differentiatorType)
    } else {
      dataSet.get
    }
  }

  def getRegisteredDataSets(): List[String] = {
    initializedDataSets.keySet.toList
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

  private def numericallyDifferentiate(input: RDD[Seq[Double]], differentiatorType: String): Map[(Int, Int), RDD[(Long, Double)]] = {
    val variablePairs = pairGenerator.generatePairs(2)

    var inputDifferentiated = Map[(Int, Int), RDD[(Long, Double)]]()

    val hdfs = FileSystem.get(new Configuration())
    for (pair <- variablePairs) {
      hdfs.delete(new Path(pairFilename(pair)), true)
    }

    for (pair <- variablePairs) {
      val differentiator = NumericalDifferentiator(differentiatorType, pair._1, pair._2)
      val differentiated = differentiator.partialDerivative(input).zipWithIndex().map(_.swap).map(row => (row._1 + 1, row._2))

      val filename = pairFilename(pair)
      differentiated.map(row => row._1.toString + "," + row._2.toString).saveAsTextFile(filename)
      val readAgain = sparkContext.textFile(filename).map(row => {
        val splitted = row.split(",")
        (splitted(0).toLong, splitted(0).toDouble)
      }).cache()
      inputDifferentiated = inputDifferentiated + (pair -> readAgain)
    }

    inputDifferentiated
  }

  private def pairFilename(pair: (Int, Int)): String = {
    "" + pair._1 + "_" + pair._2 + ".txt"
  }

  def registerDataSet(dataSetPath: String): DataSet = {
    registerDataSet(dataSetPath, "backward")
  }

  def registerDataSet(dataSetPath: String, differentiatorType: String): DataSet = {
    initializedDataSets.synchronized {
      initializedDataSets.getOrElseUpdate(dataSetPath, {
        val rdd = sparkContext.textFile(dataSetPath)

        val rowsCount = rdd.count()
        val columnsCount = if (rowsCount > 0) rdd.first().split(",").length else 0
        val parsed = rdd.filter(row => !row.startsWith("#")).map(HistdataInputParser.parseLine)

        new DataSet(
          rowsCount,
          columnsCount,
          parsed,
          numericallyDifferentiate(parsed, differentiatorType)
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
    kryo.register(classOf[HistdataTimestampParser])
    kryo.register(classOf[NumericalDifferentiator])
    kryo.register(classOf[PairGenerator])
    kryo.register(BasicFunctions.getClass)
    kryo.register(HistdataInputParser.getClass)
  }
}

object SparkContextService {
  private val executorUri = "http://d3kbcqa49mib13.cloudfront.net/spark-1.2.0-bin-hadoop2.4.tgz"
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
      .set("spark.kryo.registrator", "nibbler.api.NibblerRegistrator")

    val ctx = new SparkContext(conf)
    ctx.addJar(nibblerJarRealPath)
    ctx
  }
}