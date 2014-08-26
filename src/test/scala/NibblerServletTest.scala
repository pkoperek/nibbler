import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatra.test.scalatest.ScalatraSuite

@RunWith(classOf[JUnitRunner])
class NibblerServletTest extends ScalatraSuite with FunSuite with MockitoSugar {

  private val configuration = new SparkConf().setAppName("test").setMaster("local")
  private val sparkContext = new SparkContext(configuration)

  addServlet(new NibblerServlet(sparkContext), "/*")

  test("should return status") {
    get("/status") {
      status should equal(200)
      body should include((1 to 9).mkString(","))
    }
  }

}
