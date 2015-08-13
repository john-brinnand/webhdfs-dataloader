package webhdfs.dataloader.test;

import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import spongecell.spring.event_handler.EventHandler;
import spongecell.spring.event_handler.IEventHandler;
import spongecell.spring.event_handler.consumer.EventHandlerGenericConsumerTest;
import spongecell.spring.event_handler.exception.InvalidTranslatorException;
import spongecell.spring.event_handler.message.RequestEvent;
import webhdfs.dataloader.WebHdfs;
import webhdfs.dataloader.WebHdfsConfiguration;
import webhdfs.dataloader.application.WebHdfsDataLoaderConfiguration;

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
@ContextConfiguration(classes = { EventHandler.class })
public class WebHdfsDataLoaderEventHandlerTest extends AbstractTestNGSpringContextTests {
	@Autowired private EventHandler<String, String> eventHandler; 
	@Autowired private EventHandler<String, String> eventHandlerConsumer; 
	private final ExecutorService pool = Executors.newFixedThreadPool(1);
	private final String topic = "audience-server-bluekai"; 
	private final String key = "key.0"; 	
	private static final String GROUP = "testGroup";
	private final String msg = "validateEventHandlerFuture says - 'Greetings' "; 
	
	public EventHandlerGenericConsumerTest<String, String> loadData() throws IOException {
	    Future<EventHandlerGenericConsumerTest<String, String>> rval =  pool.submit(
	    		new Callable<EventHandlerGenericConsumerTest<String, String>>() {
	        @Override
	        public EventHandlerGenericConsumerTest<String, String> call() throws Exception {
	        	eventHandlerConsumer
	        		.keyTranslatorType(String.class)
	        		.valueTranslatorType(String.class)
	        		.groupId(GROUP)
	        		.topic(topic)
	        		.partition(0)
	        		.build();
	        	EventHandlerGenericConsumerTest<String, String>  eventConsumer = 
	    				new EventHandlerGenericConsumerTest<String, String>();
	        	eventHandlerConsumer.readAll(topic, eventConsumer);

				return eventConsumer; 
	        }
	    });
	    EventHandlerGenericConsumerTest<String, String> eventConsumer = null;
	    try {
	    	eventConsumer = rval.get();
		} catch (InterruptedException | ExecutionException e) {
			log.error("Thread interrupted.");
		}
	    return eventConsumer;
	}
	
	@Test(priority = 1, groups = "integration")
	public void validateEventHandlerFuture() throws Exception {
		Assert.assertEquals(pool.isShutdown(), false);
		
		IEventHandler<String,String> handler = eventHandler
			.groupId(GROUP)
			.topic(topic)
			.partition(0)
			.keyTranslatorType(String.class)
			.valueTranslatorType(String.class)
			.build();
		try {
			handler.write(topic, key, msg);
		} catch (InvalidTranslatorException e) {
			log.info ("ERROR - failed to write: {} ", e);
		}
		EventHandlerGenericConsumerTest<String, String> eventConsumer = loadData();
		while (eventConsumer == null || eventConsumer.getTopic() == null) {
			Thread.sleep(3000);
			loadData();
		}
    	Assert.assertEquals(eventConsumer.getTopic(), topic);
    	Assert.assertEquals(eventConsumer.getKey(), key);
    	Assert.assertEquals(eventConsumer.getValue(), msg);		
	}
	
	@AfterTest
	public void afterTest() throws InterruptedException {
		pool.shutdown();
		pool.awaitTermination(5000, TimeUnit.MILLISECONDS);
		if (!pool.isShutdown()) {
			log.info ("Pool is not shutdown.");
			pool.shutdownNow();
		}	
		Assert.assertEquals(pool.isShutdown(), true);
		log.info("Pool shutdown status : {}", pool.isShutdown());
	}
}	