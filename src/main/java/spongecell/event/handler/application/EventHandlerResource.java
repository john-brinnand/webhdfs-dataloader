package spongecell.event.handler.application;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;

import javax.servlet.http.HttpServletRequest;

import lombok.extern.slf4j.Slf4j;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.util.ByteArrayBuilder;

@Slf4j
@RestController
@RequestMapping("/v1/eventHandler")
public class EventHandlerResource {
	
	@RequestMapping(method = RequestMethod.GET)
	public ResponseEntity<?> ping(HttpServletRequest request) throws Exception {
		String body = new String ("Greetings.");
		ResponseEntity<String> response = new ResponseEntity<String>(body, HttpStatus.ACCEPTED);
		log.info("Here.");
		return response; 
	}	
	
	@RequestMapping("/ping")
	public ResponseEntity<?> icmpEcho(HttpServletRequest request) throws Exception {
		InputStream is = request.getInputStream();
		String body = getContent(is); 
		log.info("Returning : {} ", body);
		ResponseEntity<String> response = new ResponseEntity<String>(body, HttpStatus.ACCEPTED);
		return response; 
	}
	private String getContent (InputStream is) throws IOException {
		ByteArrayBuilder bab = new ByteArrayBuilder();
		int value;
		while ((value = is.read()) != -1) {
			bab.append(value);
		}
		return new String(bab.toByteArray());
	}
}
