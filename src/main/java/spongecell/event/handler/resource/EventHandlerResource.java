package spongecell.event.handler.resource;

import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/v1/audience")
public class EventHandlerResource {
	
	@RequestMapping(value = "{provider}", method = RequestMethod.GET)
	public void providerGet(@PathVariable String provider,
			HttpServletRequest request) throws Exception {
		log.info("Here.");
		return; 
	}	
}
