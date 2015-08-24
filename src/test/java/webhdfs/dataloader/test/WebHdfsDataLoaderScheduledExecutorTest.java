package webhdfs.dataloader.test;

import java.io.InputStream;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import spongecell.spring.event_handler.EventHandler;
import spongecell.spring.event_handler.IEventHandler;
import spongecell.spring.event_handler.exception.InvalidTranslatorException;
import webhdfs.dataloader.consumer.EventHandlerConsumer;
import webhdfs.dataloader.scheduler.EventHandlerJobScheduler;

/**
 *        @ContextConfiguration - among other things, it loads the application context. 
 *        If it is not present, the following error occurs:
 *        "java.lang.IllegalArgumentException: Cannot load an ApplicationContext 
 *        with a NULL 'contextLoader'. Consider annotating your test class with 
 *        @ContextConfiguration or @ContextHierarchy.at 
 *        org.springframework.util.Assert.notNull(Assert.java:112) 
 *        ~[spring-core-4.1.5.RELEASE.jar:4.1.5.RELEASE]"
 *        
 *        Note: if ContextConfiguration includes the test - in this case 
 *        WebHdfsDataLoaderEventHandlerTest, and the test extends 
 *        AbstractTestNGSpringContextTests, the test will be initialized 
 *        twice and the PostConstruct will be called two times.
 */
@Slf4j
@EnableAutoConfiguration
@ContextConfiguration(classes = { EventHandlerJobScheduler.class })
public class WebHdfsDataLoaderScheduledExecutorTest extends AbstractTestNGSpringContextTests {
	@Autowired private EventHandler<String, String> eventHandler; 
	@Autowired private EventHandler<String, String> eventHandlerConsumer; 
	private final String topic = "audience-server-bluekai"; 
	private final String key = "key.0"; 	
	private static final String GROUP = "testGroup";
	private @Autowired EventHandlerJobScheduler scheduler;
	private String data;
	
	@BeforeTest
	public void beforeTest() {
		data = readFile("/creative-event-data.json");
	}	
		
	
	@PostConstruct
	public void loadData() {
		try {
			scheduler.loadData();
		} catch (TimeoutException | InterruptedException | ExecutionException e) {
			log.info("ERROR - scheduler load data has failed: {}", e );
		}
	}
	
	@Test(priority = 1, groups = "integration")
	public void validateEventHandlerFuture() throws Exception {
		IEventHandler<String,String> handler = eventHandler
			.groupId(GROUP)
			.topic(topic)
			.partition(0)
			.keyTranslatorType(String.class)
			.valueTranslatorType(String.class)
			.build();
		try {
			for (int i = 0; i < 500; i++) {
				handler.write(topic, key, data);
			}
			handler.writerClose();
		} catch (InvalidTranslatorException e) {
			log.info ("ERROR - failed to write: {} ", e);
		}
		Thread.sleep(25000);
		EventHandlerConsumer<String, String> eventConsumer;
		do {
			eventConsumer = scheduler.getEventConsumer();
			Thread.sleep(2000);
		} while (eventConsumer.getKey() == null);
	
    	Assert.assertEquals(eventConsumer.getKey(), key);
    	Assert.assertEquals(eventConsumer.getTopic(), topic);		
    	Assert.assertEquals(eventConsumer.getValue(), data);		
	}
	
	/**
	 * Might be better to have this in a common library
	 * 
	 * @param resourceName
	 * @return
	 * @throws FileNotFoundException
	 */
	private String readFile(String resourceName) {
		InputStream inputStream = this.getClass().getResourceAsStream(
				resourceName);

		if (inputStream == null) {
			log.debug("No inputStream");
		}
		Scanner scanner = new java.util.Scanner(inputStream);
		String content = scanner.useDelimiter("\\Z").next();
		scanner.close();
		return content;
	}		
	@AfterTest
	public void afterTest() {}
}	