package spongecell.event.handler.resource;

import javax.servlet.http.HttpServletRequest;

import lombok.extern.slf4j.Slf4j;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/v1/eventHandler")
public class EventHandlerResource {
	
	@RequestMapping("/echo")
	public void ping(HttpServletRequest request) throws Exception {
		log.info("Here.");
		return; 
	}	
}
