import javax.servlet.ServletContext

import nibbler.api.{SparkContextService, NibblerServlet}
import org.scalatra._

class ScalatraBootstrap extends LifeCycle {

  override def init(servletContext: ServletContext) {
    val nibblerJarRealPath = servletContext.getRealPath("/WEB-INF/lib/nibbler.jar")
    val sparkContextService = SparkContextService(nibblerJarRealPath, "nibbler")
    servletContext.mount(new NibblerServlet(sparkContextService), "/*")
  }
}

