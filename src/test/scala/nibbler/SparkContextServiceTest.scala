package nibbler

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FunSuite}

@RunWith(classOf[JUnitRunner])
class SparkContextServiceTest
  extends FunSuite with ShouldMatchers with MockitoSugar with BeforeAndAfterEach {

  private val oldSystemProperties = System.getProperties

  private var sparkContext: SparkContext = null
  private var service: SparkContextService = null

  override protected def beforeEach() = {
    val newSystemSettings = new Properties(oldSystemProperties)
    newSystemSettings.setProperty(SparkContextService.nibblerMasterUriKey, "local")
    System.setProperties(newSystemSettings)

    sparkContext = mock[SparkContext]
    service = new SparkContextService(sparkContext)
  }

  override protected def afterEach() = {
    System.setProperties(oldSystemProperties)

    service = null
  }

  test("caches registered data set") {
    // Given

    // When
    service.registerDataSet("somePath")
    service.registerDataSet("somePath")

    // Then
    verify(sparkContext, times(1)).textFile(anyString(), anyInt())
  }

  test("registers new data set") {
    // Given
    val rdd = mock[RDD[String]]
    val requestedDataSet = "someFilePath"
    when(sparkContext.textFile(anyString(), anyInt())).thenReturn(rdd)

    // When
    val registeredDataSet = service.registerDataSet(requestedDataSet)

    // Then
    registeredDataSet should not equal null
  }

  test("get or else register data set") {
    // Given
    val rdd = mock[RDD[String]]
    val dataSetPath = "someDataSetPath"
    when(sparkContext.textFile(anyString, anyInt)).thenReturn(rdd)

    // When
    val dataSet: RDD[String] = service.getDataSetOrRegister(dataSetPath)

    // Then
    dataSet should equal(rdd)
  }

  test("registered data set is reported as contained") {
    // Given
    val dataSetPath = "someDataSetFilePath"

    // When
    service.registerDataSet(dataSetPath)
    val dataSetContained = service.containsDataSet(dataSetPath)

    // Then
    dataSetContained should equal(true)
  }

  test("not registered data set should be reported as not contained") {
    // When
    val dataSetContained = service.containsDataSet("someDataSetFilePath")

    // Then
    dataSetContained should equal(false)
  }

  test("unregistering not registered data set doesn't call anything in spark context") {
    // Given

    // When
    service.unregisterDataSet("not registered data set")

    // Then
    verifyZeroInteractions(sparkContext)
  }

  test("unregistering not registered data set doesn't throw exception") {
    try {
      service.unregisterDataSet("not registered data set")
    }
    catch {
      case _ => fail("exception shouldn't be thrown")
    }
  }

  test("unregistering data set removes from cache") {
    // Given
    val dataSetPath = "someDataSetFilePath"

    // When
    service.registerDataSet(dataSetPath)
    service.unregisterDataSet(dataSetPath)
    val dataSetContained = service.containsDataSet(dataSetPath)

    // Then
    dataSetContained should equal(false)
  }

  test("provides raw spark context") {
    // Given

    // When
    val retrievedSparkContext = service.getSparkContext

    // Then
    retrievedSparkContext should equal(sparkContext)
  }
}
