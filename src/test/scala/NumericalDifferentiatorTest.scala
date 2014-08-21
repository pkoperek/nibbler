import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import spray.json._
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.verify
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

@RunWith(classOf[JUnitRunner])
class NumericalDifferentiatorTest extends FunSuite with ShouldMatchers with MockitoSugar  {

  test("accepts 'backward' as differentiator strategy") {
    try { 
      NumericalDifferentiator("backward", 0, 1)
    } catch {
      case _: Exception => fail("shouldn't throw an exception!")
    }
  }

  test("accepts 'central' as differentiator strategy") {
    try { 
      NumericalDifferentiator("central", 0, 1)
    } catch {
      case _: Exception => fail("shouldn't throw an exception!")
    }
  }

  test("indexes of variables cannot be the same") {
    intercept[IllegalArgumentException] {
      NumericalDifferentiator("central", 1, 1)
    }
  }

  test("backward: differentiates the rdd according to formula") {
    // Given
    val differentiator = NumericalDifferentiator("backward", 0, 1)
    val configuration = new SparkConf().setAppName("test").setMaster("local")
    val sparkContext = new SparkContext(configuration)
    val input: RDD[Seq[Double]] = sparkContext.parallelize(List(List(10.0), List(20.0), List(30.0), List(40.0), List(50.0), List(60.0), List(70.0), List(80.0), List(90.0), List(100.0)))

    // When
    val result = differentiator.partialDerivative(input).collect()

    // Then
    result should equal (Array(20.0, 30.0, 40.0, 50.0))
  }

}
