import org.scalatra._
import scalate.ScalateSupport

class NibblerServlet extends ScalatraServlet {

  get("/status") {
    "Hello world!"
  }

  post("/evaluate") {
    params
  }

}

