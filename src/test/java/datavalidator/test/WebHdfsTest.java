package datavalidator.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.PostConstruct;

import kafka.api.Request;
import lombok.extern.slf4j.Slf4j;

import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Slf4j
@ContextConfiguration(classes = { WebHdfsTest.class, WebHdfs.class})
@EnableConfigurationProperties ({ WebHdfsConfiguration.class })
public class WebHdfsTest extends AbstractTestNGSpringContextTests{
	@Autowired WebHdfsConfiguration webHdfsConfig;
	@Autowired WebHdfs webHdfs;
	private URI uri;
	
	@BeforeTest
	public void beforeTest() { }
	@PostConstruct
	public void postConstruct() throws URISyntaxException {
		uri = new URIBuilder()
			.setScheme(webHdfsConfig.getScheme())
			.setHost(webHdfsConfig.getHost())
			.setPort(webHdfsConfig.getPort())
			.setPath(webHdfsConfig.getWEBHDFS_PREFIX()
				+ webHdfsConfig.getPath() + "/" 
				+ webHdfsConfig.getFileName())
			.setParameter("overwrite", "true")
			.setParameter("op", "CREATE")
			.setParameter("user", "spongecell")
			.build();
	}
	
	@Test
	public void validateCloseableHttpClient() throws URISyntaxException,
			ClientProtocolException, IOException {
		StringEntity entity = new StringEntity("Greetings earthling.");
		HttpPut put = new HttpPut(uri);
		put.setEntity(entity);
		log.info (put.getURI().toString());
		log.info (put.getEntity().toString());
		
		CloseableHttpClient httpClient = HttpClients.createDefault(); 
		
		CloseableHttpResponse response = httpClient.execute(put);
		Assert.assertNotNull(response);
		log.info("Response status code {} ", response.getStatusLine().getStatusCode());
		Assert.assertEquals(307, response.getStatusLine().getStatusCode());
		response.close();
		httpClient.close();
	}
}
