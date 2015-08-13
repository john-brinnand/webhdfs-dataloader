package webhdfs.dataloader.application;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.servlet.http.HttpServletRequest;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import spongecell.spring.event_handler.EventHandler;
import spongecell.spring.event_handler.consumer.EventHandlerGenericConsumerTest;
import spongecell.spring.event_handler.message.RequestEvent;

import com.fasterxml.jackson.core.util.ByteArrayBuilder;

@Slf4j
@RestController
@RequestMapping("/v1/eventHandler")
public class WebHdfsDataLoaderResource {
	@Autowired private EventHandler<String, RequestEvent> eventHandler; 
	ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);

	public void runLoader() throws IOException, InterruptedException {
		do {
			EventHandlerGenericConsumerTest<String, RequestEvent> eventConsumer = loadData();
			if (eventConsumer == null || eventConsumer.getTopic() == null) {
				Thread.sleep(3000);
			}
		} while (pool.isShutdown() == false);
	}
	
	@PostConstruct
	private EventHandlerGenericConsumerTest<String, RequestEvent> loadData() throws IOException {
	    Future<EventHandlerGenericConsumerTest<String, RequestEvent>> rval =  pool.schedule(
	    	new Callable<EventHandlerGenericConsumerTest<String, RequestEvent>>() {
		        @Override
		        public EventHandlerGenericConsumerTest<String, RequestEvent> call() throws Exception {
		        	log.info("Running in callable.");
		        	eventHandler
		        		.keyTranslatorType(String.class)
		        		.valueTranslatorType(RequestEvent.class)
		        		.build();
		        	EventHandlerGenericConsumerTest<String, RequestEvent>  eventConsumer = 
		    				new EventHandlerGenericConsumerTest<String, RequestEvent>();
		        	eventHandler.readAll("", eventConsumer);
	
					return eventConsumer; 
		        }
	    }, 1000, TimeUnit.MILLISECONDS);
	    log.info("Returned from callable.");
//	    EventHandlerGenericConsumerTest<String, RequestEvent> eventConsumer = null;
//	    try {
//	    	eventConsumer = rval.get();
//		} catch (InterruptedException | ExecutionException e) {
//			log.error("Thread interrupted.");
//		}
	    return null;
	}
	
	@PreDestroy 
	public void shutdown () throws InterruptedException {
		pool.shutdown();
		pool.awaitTermination(5000, TimeUnit.MILLISECONDS);
		if (!pool.isShutdown()) {
			log.info ("Pool is not shutdown.");
			pool.shutdownNow();
		}	
		Assert.isTrue(pool.isShutdown());
		log.info("Pool shutdown status : {}", pool.isShutdown());
	}
	
	@RequestMapping("/ping")
	public ResponseEntity<?> icmpEcho(HttpServletRequest request) throws Exception {
		InputStream is = request.getInputStream();
		String content = getContent(is); 
		log.info("Returning : {} ", content);
		ResponseEntity<String> response = new ResponseEntity<String>(content, HttpStatus.OK);
		return response; 
	}
	
	@RequestMapping(method = RequestMethod.POST)
	public ResponseEntity<?> postRequestParamEndpoint(HttpServletRequest request,
			@RequestParam(value = "id") String id) throws Exception {
		String content = "Greetings " + id  + " from the postRequestParamEndpoint"; 
		log.info("Returning : {} ", content);
		ResponseEntity<String> response = new ResponseEntity<String>(content, HttpStatus.OK);
		return response; 
	}	
	
	@RequestMapping(method = RequestMethod.PUT)
	public ResponseEntity<?> eventHandlerAdmin(HttpServletRequest request,
		 @RequestParam String op) throws Exception {
		String content = "Greetings eventHandlerAdministrator."; 
		if (op.equals("start")) {
			// TODO add Future here.
		}
		log.info("Returning : {} ", content);
		ResponseEntity<String> response = new ResponseEntity<String>(content, HttpStatus.OK);
		return response; 
	}	
	
	@RequestMapping(method = RequestMethod.GET)
	public ResponseEntity<?> getRequestParamEndpoint(HttpServletRequest request,
			@RequestParam(value = "id") String id) throws Exception {
		String content =  id + ":" + "testValue";
		log.info("Returning {} for id {}", content, id);
		ResponseEntity<String> response = new ResponseEntity<String>(content, HttpStatus.OK);
		return response; 
	}	
	
	@RequestMapping(method = RequestMethod.DELETE)
	public ResponseEntity<?> deleteRequestParamEndpoint(HttpServletRequest request,
			@RequestParam(value = "id") String id) throws Exception {
		String content =  "Deleted " + id + ":" + "testValue";
		log.info("Returning {} for id {}", content, id);
		ResponseEntity<String> response = new ResponseEntity<String>(content, HttpStatus.OK);
		return response; 
	}	
	
	private String getContent (InputStream is) throws IOException {
		ByteArrayBuilder bab = new ByteArrayBuilder();
		int value;
		while ((value = is.read()) != -1) {
			bab.append(value);
		}
		String content = new String(bab.toByteArray());
		bab.close();
		return content; 
	}
}
