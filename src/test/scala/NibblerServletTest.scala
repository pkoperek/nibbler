import org.apache.spark.SparkContext
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.junit.Ignore
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.scalatest.FunSuiteLike
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatra.test.scalatest.ScalatraSuite

@Ignore
@RunWith(classOf[JUnitRunner])
class NibblerServletTest extends ScalatraSuite with FunSuiteLike with MockitoSugar {

  val sparkContext = mock[SparkContext]

  addServlet(classOf[NibblerServlet], "/*")

  override def baseUrl: String =
    server.getConnectors collectFirst {
      case conn: SelectChannelConnector =>
        val host = Option(conn.getHost) getOrElse "localhost"
        val port = conn.getLocalPort
        val url = "http://%s:%d".format(host, port)
        println("Using url: " + url)
        url
    } getOrElse sys.error("couldn't find a matching connector")

  test("get is unsupported in evaluate") {
    get("/evaluate") {
      status should equal (405)
    }
  }

  test("should evaluate test file") {
    val jsonRequest =
      """
      {
        "inputFile": "test.txt",
        "function":
        {
           "function": "sin",
           "operands": [{
               "function": "1.0"
           }]
        }
      }
      """

    post("/evaluate") {
      status should equal (200)
    }
  }

}
