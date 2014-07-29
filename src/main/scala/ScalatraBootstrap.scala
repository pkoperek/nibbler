import org.scalatra._
import javax.servlet.ServletContext

class ScalatraBootstrap extends LifeCycle {

    override def init(servletContext: ServletContext) {
        val nibblerJarRealPath = servletContext.getRealPath("/WEB-INF/lib/nibbler.jar")

        servletContext.mount(new NibblerServlet(new SparkContextService(nibblerJarRealPath)), "/*")
    }
}

