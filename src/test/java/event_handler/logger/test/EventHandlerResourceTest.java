package event_handler.logger.test;

import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
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
import org.testng.Assert;
import org.testng.annotations.Test;

import spongecell.event.handler.application.EventHandlerResourceConfiguration;

@Slf4j
@EnableAutoConfiguration
@EnableWebMvc
@WebAppConfiguration
@ContextConfiguration(classes = { spongecell.event.handler.application.EventHandlerResourceApplication.class })
public class EventHandlerResourceTest extends AbstractTestNGSpringContextTests {
	private String data;
	private final static String BASE_URI = "/v1/eventHandler";
	private final static String PING = "ping";
	@Autowired WebApplicationContext wac;
	@Autowired EventHandlerResourceConfiguration config;

	@Test(priority = 1, groups = "integration")
	public void validateEventHandlerPing() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
		data = "Greetings!";

		MockHttpServletRequestBuilder request = MockMvcRequestBuilders
				.post(BASE_URI + "/" + PING);
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
		Assert.assertEquals(mvcResult.getResponse().getContentAsString(), data);
	}
}
