package nibbler

import java.util.Properties

import nibbler.api.SparkContextService
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FunSuite}

@RunWith(classOf[JUnitRunner])
class SparkContextServiceTest
  extends FunSuite with ShouldMatchers with MockitoSugar with BeforeAndAfterEach {

  private val oldSystemProperties = System.getProperties

  private val dataSetPath = "dataSetPath"
  private val dataSetNumberOfRows = 314
  private val dataSetNumberOfColumns = 5

  private var sparkContext: SparkContext = null
  private var service: SparkContextService = null
  private var dataSetRDD: RDD[String] = null

  override protected def beforeEach() = {
    val newSystemSettings = new Properties(oldSystemProperties)
    newSystemSettings.setProperty(SparkContextService.nibblerMasterUriKey, "local")
    System.setProperties(newSystemSettings)

    val parsedRDD = mock[RDD[Seq[Double]]]
    sparkContext = mock[SparkContext]
    dataSetRDD = mock[RDD[String]](returnRddForMap(parsedRDD))
    when(dataSetRDD.count()).thenReturn(dataSetNumberOfRows)
    when(dataSetRDD.first()).thenReturn("1,2,3,4,5")
    when(sparkContext.textFile(anyString(), anyInt())).thenReturn(dataSetRDD)

    service = new SparkContextService(sparkContext)
  }

  override protected def afterEach() = {
    System.setProperties(oldSystemProperties)

    service = null
  }

  test("caches registered data set") {
    // Given

    // When
    service.registerDataSet(dataSetPath)
    service.registerDataSet(dataSetPath)

    // Then
    verify(sparkContext, times(1)).textFile(anyString(), anyInt())
  }

  test("registers new data set") {
    // Given
    val rdd = mock[RDD[String]](RETURNS_DEEP_STUBS)
    val requestedDataSet = "someFilePath"
    when(sparkContext.textFile(anyString(), anyInt())).thenReturn(rdd)

    // When
    val registeredDataSet = service.registerDataSet(requestedDataSet)

    // Then
    registeredDataSet should not equal null
  }

//  test("get or else register data set") {
//    // Given
//    val cachedRdd = mock[RDD[Seq[Double]]]
//    val parsedRdd = mock[RDD[Seq[Double]]](returnRddForCache(cachedRdd))
//    val rdd = mock[RDD[String]](returnRddForMap(parsedRdd))
//    val dataSetPath = "someDataSetPath"
//    when(sparkContext.textFile(anyString, anyInt)).thenReturn(rdd)
//
//    // When
//    val dataSet = service.getDataSetOrRegister(dataSetPath)
//
//    // Then
//    dataSet.getRawData should not equal rdd
//    dataSet.getRawData should equal(cachedRdd)
//  }

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
    // When
    val retrievedSparkContext = service.getSparkContext

    // Then
    retrievedSparkContext should equal(sparkContext)
  }

  test("counts rows of data set") {
    // When
    val registrationResult = service.registerDataSet(dataSetPath)

    // Then
    registrationResult.getNumberOfRows should equal(dataSetNumberOfRows)
  }

  test("counts columns of data set") {
    // When
    val registrationResult = service.registerDataSet(dataSetPath)

    // Them
    registrationResult.getNumberOfColumns should equal(dataSetNumberOfColumns)
  }

  private def returnRddForMap(otherMock: RDD[Seq[Double]]): Answer[RDD[Seq[Double]]] = {
    new Answer[RDD[Seq[Double]]]() {
      override def answer(invocation: InvocationOnMock): RDD[Seq[Double]] = {
        if (invocation.getMethod.getName.equals("map"))
          otherMock
        else
          null
      }
    }
  }

  def returnRddForCache(otherMock: RDD[Seq[Double]]) = {
    new Answer[RDD[Seq[Double]]]() {
      override def answer(invocation: InvocationOnMock): RDD[Seq[Double]] = {
        if (invocation.getMethod.getName.equals("cache"))
          otherMock
        else
          null
      }
    }
  }
}
