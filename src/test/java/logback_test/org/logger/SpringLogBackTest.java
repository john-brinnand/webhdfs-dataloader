package logback_test.org.logger;

import lombok.extern.slf4j.Slf4j;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@Slf4j
@ContextConfiguration(classes = { logback_test.org.logger.EventHandlerResourceApplication.class })
public class SpringLogBackTest extends AbstractTestNGSpringContextTests{
	@Test
	public void testLogger() {
		log.info("Testing 123");
		log.info("Here");
	}
}
