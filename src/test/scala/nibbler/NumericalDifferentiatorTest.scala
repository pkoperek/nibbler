package nibbler

import org.apache.spark.rdd.RDD
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
@Ignore
class NumericalDifferentiatorTest extends FunSuite with ShouldMatchers with MockitoSugar with SparkContextAware {

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

  test("validates input data set has at least two variables") {
    // Given
    val differentiator = NumericalDifferentiator("backward", 0, 1)
    val input: RDD[Seq[Double]] = sparkContext.parallelize(List(List(10.0, 20.0)))

    // Then
    try {
      differentiator.partialDerivative(input)
    } catch {
      case _: Exception => fail("no exception should be thrown")
    }
  }

  test("accepts data set with two variables") {
    // Given
    val differentiator = NumericalDifferentiator("backward", 0, 1)
    val input: RDD[Seq[Double]] = sparkContext.parallelize(List(List(10.0)))

    // Then
    intercept[IllegalArgumentException] {
      differentiator.partialDerivative(input)
    }
  }

  test("backward: differentiates the rdd according to formula") {
    // Given
    val differentiator = NumericalDifferentiator("backward", 0, 1)
    val input: RDD[Seq[Double]] = sparkContext.parallelize(List(List(10.0, 1.0), List(20.0, 3.0)))

    // When
    val result = differentiator.partialDerivative(input).collect()

    // Then
    result should equal(Array(5.0))
  }

  test("central: differentiates the rdd according to formula") {
    // Given
    val differentiator = NumericalDifferentiator("central", 0, 1)
    val input: RDD[Seq[Double]] = sparkContext.parallelize(List(List(10.0, 1.0), List(100.0, 100.0), List(20.0, 5.0)))

    // When
    val result = differentiator.partialDerivative(input).collect()

    // Then
    result should equal(Array(2.5))
  }

}
