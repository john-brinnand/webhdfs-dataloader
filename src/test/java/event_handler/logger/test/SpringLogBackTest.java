package event_handler.logger.test;

import lombok.extern.slf4j.Slf4j;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.context.web.WebAppConfiguration;
import org.testng.annotations.Test;

/**
 * Notes: @WebAppConfiguration sets up, among other things,
 *        the servlet context. Without it, the following error
 *        occurs: "A ServletContext is required to configure default servlet handling"
 *        
 *        @ContextConfiguration - among other things, it loads the application context. 
 *        If it is not present, the following error occurs:
 *        "java.lang.IllegalArgumentException: Cannot load an ApplicationContext 
 *        with a NULL 'contextLoader'. Consider annotating your test class with 
 *        @ContextConfiguration or @ContextHierarchy.at 
 *        org.springframework.util.Assert.notNull(Assert.java:112) 
 *        ~[spring-core-4.1.5.RELEASE.jar:4.1.5.RELEASE]"
 */

@Slf4j(topic="event-handler-service-logger")
@ContextConfiguration(classes = { spongecell.event.handler.application.EventHandlerResourceApplication.class })
@WebAppConfiguration
public class SpringLogBackTest extends AbstractTestNGSpringContextTests {
	@Test
	public void testLogger() {
		log.info("Testing 123");
		log.info("Here");
	}
}
