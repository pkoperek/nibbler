import javax.servlet.ServletContext

import nibbler.api.{SparkContextService, NibblerServlet}
import org.scalatra._

class ScalatraBootstrap extends LifeCycle {

  override def init(servletContext: ServletContext) {
    val nibblerJarRealPath = servletContext.getRealPath("/WEB-INF/lib/nibbler.jar")
    val jodaTimePath = servletContext.getRealPath("/WEB-INF/lib/joda-time-2.2.jar")
    val sparkContextService = SparkContextService(Array(nibblerJarRealPath, jodaTimePath), "nibbler")
    servletContext.mount(new NibblerServlet(sparkContextService), "/*")
  }
}

