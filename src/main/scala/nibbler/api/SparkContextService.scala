package nibbler.api

import org.apache.spark.SparkContext._
import com.esotericsoftware.kryo.Kryo
import nibbler.evaluation._
import nibbler.io.{HistdataInputParser, HistdataTimestampParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark._


import scala.collection.mutable
import scala.reflect.ClassTag

class SparkContextService(sparkContext: SparkContext) extends Serializable with Logging {

  private val initializedDataSets = mutable.Map[String, DataSet]()
  private val pairGenerator = new PairGenerator
  private val tmpDirectory = sparkContext.getConf.get("nibbler.hdfs.tmp.dir")
  private val hadoopConfigDirectory = sparkContext.getConf.get("hadoop.conf.dir")

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

  private def serialize[RDDType: ClassTag](path: String, input: RDD[RDDType]): RDD[RDDType] = {
    input.saveAsObjectFile(path)
    sparkContext.objectFile[RDDType](path).cache()
  }

  private def deleteFile(path: String) = {
    val configuration: Configuration = new Configuration()
    configuration.addResource(new Path(hadoopConfigDirectory, "core-site.xml"))
    configuration.addResource(new Path(hadoopConfigDirectory, "hdfs-site.xml"))
    val fileSystem = FileSystem.get(configuration)

    fileSystem.delete(new Path(tmpDirectoryPrefix(), path), true)
  }

  private def numericallyDifferentiate(input: RDD[Seq[Double]], differentiatorType: String): Map[(Int, Int), RDD[(Seq[Double], Double)]] = {
    val variablePairs = pairGenerator.generatePairs(2)

    var inputDifferentiated = Map[(Int, Int), RDD[(Seq[Double], Double)]]()

    for (pair <- variablePairs) {
      deleteFile(input.name + pairSuffix(pair))
    }

    val inputWithIndex = input.zipWithIndex().map(_.swap)
    inputWithIndex.setName(input.name + "-withIndex")

    for (pair <- variablePairs) {
      val differentiator = NumericalDifferentiator(differentiatorType, pair._1, pair._2)
      val inputAndDifferentiated = differentiator.partialDerivative(inputWithIndex)

      val filename = tmpDirectoryPrefix() + "/" + inputWithIndex.name + pairSuffix(pair)
      val readAgain = serialize(filename, inputAndDifferentiated)

      inputDifferentiated = inputDifferentiated + (pair -> readAgain)
    }

    inputDifferentiated
  }

  private def tmpDirectoryPrefix(): String = {
    val configuration: Configuration = new Configuration()
    configuration.addResource(new Path(hadoopConfigDirectory, "core-site.xml"))
    configuration.addResource(new Path(hadoopConfigDirectory, "hdfs-site.xml"))
    val fileSystem = FileSystem.get(configuration)

    if (fileSystem.isInstanceOf[LocalFileSystem]) {
      return "/tmp"
    }

    fileSystem.getUri.toString + "/" + tmpDirectory
  }

  private def pairSuffix(pair: (Int, Int)): String = {
    "_" + pair._1 + "_" + pair._2 + ".txt"
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
        parsed.setName(new Path(dataSetPath).getName)

        new DataSet(
          rowsCount,
          columnsCount,
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
  private val executorUri = "http://d3kbcqa49mib13.cloudfront.net/spark-1.2.1-bin-hadoop2.4.tgz"
  private val defaultMasterUri = "mesos://zk://localhost:2181/mesos"
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
      .set("spark.executor.extraClassPath", "/etc/hadoop/conf")
      .set("spark.executor.memory", executorMemory)
      .setSparkHome("someHomeWhichShouldBeIrrelevant")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "nibbler.api.NibblerRegistrator")
      .set("nibbler.hdfs.tmp.dir", "/tmp")
      .set("spark.mesos.coarse", "true")
      .set("spark.mesos.executor.memoryOverhead", "100")
      .set("hadoop.conf.dir", "/etc/hadoop/conf")

    val ctx = new SparkContext(conf)
    ctx.addJar(nibblerJarRealPath)
    ctx
  }
}
