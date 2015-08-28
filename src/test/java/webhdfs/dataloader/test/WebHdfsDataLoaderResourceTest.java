package webhdfs.dataloader.test;

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

import webhdfs.dataloader.application.WebHdfsDataLoaderConfiguration;

/**
 * Notes: @WebAppConfiguration sets up, among other things,
 *        the servlet context. Without it, the following error
 *        occurs: "A ServletContext is required to configure default servlet handling"
 *        
 *        @ContextConfiguration - among other things, it loads the application context. 
 *        If it is not present, the following error occurs:
 *        "java.lang.IllegalArgumentException: Cannot load an ApplicationContext 
 *        with a NULL 'contextLoader'. Consider annotating your test class with 
 *        @ContextConfiguration or @ContextHierarchy.at 
 *        org.springframework.util.Assert.notNull(Assert.java:112) 
 *        ~[spring-core-4.1.5.RELEASE.jar:4.1.5.RELEASE]"
 */
@Slf4j
@EnableAutoConfiguration
@EnableWebMvc
@WebAppConfiguration
@ContextConfiguration(classes = { webhdfs.dataloader.application.WebHdfsDataLoaderApplication.class })
public class WebHdfsDataLoaderResourceTest extends AbstractTestNGSpringContextTests {
	private String data;
	private final static String BASE_URI = "/v1/webhdfsDataloader";
	private final static String PING = "ping";
	@Autowired WebApplicationContext wac;
	@Autowired WebHdfsDataLoaderConfiguration config;

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

		log.info("Raw Response - type is {} content is: {} ", 
			mvcResult.getResponse().getContentType(), 
			mvcResult.getResponse().getContentAsString());
		Assert.assertEquals(mvcResult.getResponse().getContentAsString(), data);
		// Temporary
		Thread.sleep(20000);
	}
	
	@Test(priority = 1, groups = "integration")
	public void validatePostRequestParams() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
		data = "Greetings!";
		final String senderId = "validatePostRequestParams";

		MockHttpServletRequestBuilder request = MockMvcRequestBuilders
				.post(BASE_URI);
		request.contentType(MediaType.ALL_VALUE);
		request.content(data);
		
		String[] topics = new String[10];
		topics[0] = "audience_server_test_provider";
		request.param("topics", topics);

		ResultActions actions = mockMvc.perform(request);
		actions.andDo(print());

		// Get the result and convert it to an AudienceResponse.
		// *******************************************************
		MvcResult mvcResult = actions.andReturn();

		log.info("Raw Response - type is {} content is: {} ", 
			mvcResult.getResponse().getContentType(), 
			mvcResult.getResponse().getContentAsString());
		
		Assert.assertEquals(mvcResult.getResponse().getContentAsString(),
				"Greetings " + senderId + " from the postRequestParamEndpoint");
	}	
	
	@Test(priority = 1, groups = "integration")
	public void validateGetRequestParams() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
		final String senderId = "validateGetRequestParams";

		MockHttpServletRequestBuilder request = MockMvcRequestBuilders
				.get(BASE_URI);
		request.contentType(MediaType.ALL_VALUE);
		request.param("id", senderId);

		ResultActions actions = mockMvc.perform(request);
		actions.andDo(print());

		// Get the result and convert it to an AudienceResponse.
		// *******************************************************
		MvcResult mvcResult = actions.andReturn();

		log.info("Raw Response - type is {} content is: {} ", 
			mvcResult.getResponse().getContentType(), 
			mvcResult.getResponse().getContentAsString());
		
		Assert.assertEquals(mvcResult.getResponse().getContentAsString(),
				senderId + ":" + "testValue");
	}	
	
	@Test(priority = 1, groups = "integration")
	public void validateDeleteRequestParams() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
		final String senderId = "validateGetRequestParams";

		MockHttpServletRequestBuilder request = MockMvcRequestBuilders
				.delete(BASE_URI);
		request.contentType(MediaType.ALL_VALUE);
		request.param("id", senderId);

		ResultActions actions = mockMvc.perform(request);
		actions.andDo(print());

		// Get the result and convert it to an AudienceResponse.
		// *******************************************************
		MvcResult mvcResult = actions.andReturn();

		log.info("Raw Response - type is {} content is: {} ", 
			mvcResult.getResponse().getContentType(), 
			mvcResult.getResponse().getContentAsString());
		
		Assert.assertEquals(mvcResult.getResponse().getContentAsString(),
				"Deleted " + senderId + ":" + "testValue");
	}		
}
