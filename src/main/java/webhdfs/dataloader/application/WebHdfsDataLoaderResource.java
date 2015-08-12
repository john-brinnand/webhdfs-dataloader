package webhdfs.dataloader.application;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import spongecell.spring.event_handler.EventHandler;
import spongecell.spring.event_handler.message.RequestEvent;

import com.fasterxml.jackson.core.util.ByteArrayBuilder;

@Slf4j
@RestController
@RequestMapping("/v1/eventHandler")
public class WebHdfsDataLoaderResource {
	@Autowired private EventHandler eventHandler; 

	@PostConstruct
	public void init() {
		eventHandler.keyTranslatorType(String.class)
			.valueTranslatorType(RequestEvent.class)
			.build();
		// TODO start the event handler in a thread here.
		//************************************************
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
