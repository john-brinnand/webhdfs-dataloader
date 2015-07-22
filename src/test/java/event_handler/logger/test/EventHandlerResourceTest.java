package event_handler.logger.test;

import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;

import javax.annotation.PostConstruct;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
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
import org.testng.annotations.Test;

import spongecell.event.handler.application.EventHandlerResourceConfiguration;
import spongecell.message.RequestEvent;
import spongecell.spring.event_handler.EventHandler;

@Slf4j
@EnableAutoConfiguration
@EnableWebMvc
@WebAppConfiguration
@ContextConfiguration(classes = { spongecell.event.handler.application.EventHandlerResourceApplication.class })
public class EventHandlerResourceTest extends AbstractTestNGSpringContextTests {
	private String data;
	@Autowired WebApplicationContext wac;
	@Autowired EventHandlerResourceConfiguration config;
	private static final String GROUP = "testGroup";
	private EventHandler<String, RequestEvent> eventHandler;

	@Test
	public void testLogger() {
		log.info("Testing 123");
		log.info("Here");
	}

	@PostConstruct
	public void postConstruct() {
		// eventHandler = new EventHandler.Builder<String, RequestEvent>()
		// .keyTranslatorType(String.class)
		// .valueTranslatorType(RequestEvent.class)
		// .topic(config.getTopic())
		// .groupId(GROUP)
		// .build();
	}

	@Test(priority = 1, groups = "integration")
	public void validateEventHandlerPing() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
		data = "Greetings!";

		MockHttpServletRequestBuilder request = MockMvcRequestBuilders
//				.get("/v1/eventHandler");
				.get("/v1/eventHandler/ping");
		request.contentType(MediaType.ALL_VALUE);
		request.content(data);

		ResultActions actions = mockMvc.perform(request);
		actions.andDo(print());

		// Get the result and convert it to an AudienceResponse.
		// *******************************************************
		MvcResult mvcResult = actions.andReturn();

		log.info("Raw Response - type is {} content is: {} ", mvcResult
				.getResponse().getContentType(), mvcResult.getResponse()
				.getContentAsString());
	}
}
