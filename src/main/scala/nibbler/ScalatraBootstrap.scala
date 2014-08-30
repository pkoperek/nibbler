package nibbler

import javax.servlet.ServletContext

import org.scalatra._

class ScalatraBootstrap extends LifeCycle {

  override def init(servletContext: ServletContext) {
    val nibblerJarRealPath = servletContext.getRealPath("/WEB-INF/lib/nibbler.jar")
    val sparkContextService = new SparkContextService(nibblerJarRealPath)
    val sparkContext = sparkContextService.createSparkContext("nibbler")
    servletContext.mount(new NibblerServlet(sparkContext), "/*")
  }
}

