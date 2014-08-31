package nibbler

import java.io.File
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.mockito.Matchers
import org.mockito.Matchers._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class SparkContextServiceTest extends FunSuite with ShouldMatchers with MockitoSugar with BeforeAndAfterAll {

  private val sparkContext = mock[SparkContext]
  private val oldSystemProperties = System.getProperties

  override protected def beforeAll() = {
    val newSystemSettings = new Properties(oldSystemProperties)
    newSystemSettings.setProperty(SparkContextService.nibblerMasterUriKey, "local")
    System.setProperties(newSystemSettings)
  }

  override protected def afterAll() = {
    System.setProperties(oldSystemProperties)
  }

  test("registers new data set") {
    // Given
    val rdd = mock[RDD[String]]
    val requestedDataSet = File.createTempFile("nibbler", "nothing")
    requestedDataSet.deleteOnExit()
    when(sparkContext.textFile(anyString(),anyInt())).thenReturn(rdd)

    // When
    val retrievedDataSet: RDD[String] = new SparkContextService(sparkContext).registerDataSet(requestedDataSet.getAbsolutePath)

    // Then
    retrievedDataSet should not equal null
  }

}
