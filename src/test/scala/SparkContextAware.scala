import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Suite, AbstractSuite, BeforeAndAfterEach}

trait SparkContextAware extends BeforeAndAfterEach {

  this: Suite =>

  private val configuration = new SparkConf().setAppName("test").setMaster("local")
  protected var sparkContext: SparkContext = null

  override protected def beforeEach(): Unit = {
    sparkContext = new SparkContext(configuration)
  }

  override protected def afterEach(): Unit = {
    sparkContext.stop()
  }

  protected def textFile(inputFilePath: String) = {
    sparkContext.textFile(inputFilePath)
  }
}
