package logback_test.org.logger;


import lombok.extern.slf4j.Slf4j;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

@Slf4j
public class TestLogBack {
	@BeforeTest
	public void beforeTest() {
	}

	@AfterTest
	public void afterTest() {
	}

	@Test
	public void testLogger () {
		log.info("Testing 123");
		log.info("Here");
	}
}
