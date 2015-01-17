package nibbler

import java.io.File

import nibbler.api.{SparkContextService, NibblerServlet}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatra.test.scalatest.ScalatraSuite

@RunWith(classOf[JUnitRunner])
class NibblerServletTest extends ScalatraSuite with FunSuite with MockitoSugar with BeforeAndAfterEach {

  private val configuration = new SparkConf().setAppName("test").setMaster("local")
  private var sparkContext: SparkContext = null

  override protected def beforeEach(): Unit = {
    sparkContext = new SparkContext(configuration)
    addServlet(new NibblerServlet(new SparkContextService(sparkContext)), "/*")
  }

  override protected def afterEach(): Unit = {
    sparkContext.stop()
  }

  test("should fail registering not existing data set") {
    post("/register", body = "{ \"inputFile\": \"iDontExist\" }") {
      status should equal(500)
    }
  }

  test("data set registration completes") {
    val dataSetFile = File.createTempFile("nibbler", "suffix")
    dataSetFile.deleteOnExit()

    val requestBody = "{ \"inputFile\": \"" + dataSetFile.getAbsolutePath + "\" }"

    post("/register", body = requestBody.getBytes) {
      status should equal(200)
      body should (include("numberOfRows") and include("numberOfColumns") and include("0"))
    }
  }

  test("should return status") {
    get("/status") {
      status should equal(200)
      body should include((1 to 9).mkString(","))
    }
  }

}
