import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class NibblerServletTest extends FunSuite with MockitoSugar with ShouldMatchers {

  test("should return status") {
    // Given
    val expectedOutput = (1 to 9).mkString(",")


  }

}
