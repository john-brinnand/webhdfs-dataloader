package event_handler.logger.test;

import lombok.extern.slf4j.Slf4j;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.context.web.WebAppConfiguration;
import org.testng.annotations.Test;

@Slf4j
@ContextConfiguration(classes = { spongecell.event.handler.application.EventHandlerResourceApplication.class })
@WebAppConfiguration
public class EventHandlerResourceTest extends AbstractTestNGSpringContextTests {
	@Test
	public void testLogger() {
		log.info("Testing 123");
		log.info("Here");
	}
}
