package se


import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.Micronaut
import org.slf4j.Logger
import org.slf4j.LoggerFactory

//@CompileStatic
class Application {

  private static final Logger logger = LoggerFactory.getLogger(Application.class);

  static void main(String[] args) {
    ApplicationContext appContext = Micronaut.build(args)
        .eagerInitSingletons(true)
        .packages("se.domain")
        .mainClass(Application.class)
        .start()
    //appContext.getBean(SocketSessionEvents.class).selfcheck()
  }
}
