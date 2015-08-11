package datavalidator.test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.PostConstruct;

import lombok.extern.slf4j.Slf4j;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import datavalidator.webhdfs.WebHdfs;
import datavalidator.webhdfs.WebHdfsConfiguration;
import datavalidator.webhdfs.exception.WebHdfsException;

@Slf4j
@ContextConfiguration(classes = { WebHdfsTest.class, WebHdfs.Builder.class})
@EnableConfigurationProperties ({ WebHdfsConfiguration.class })
public class WebHdfsTest extends AbstractTestNGSpringContextTests{
	@Autowired WebHdfsConfiguration webHdfsConfig;
	@Autowired WebHdfs.Builder webHdfsBuilder;
	private URI uri;

	@PostConstruct
	public void postConstruct() throws URISyntaxException {
		uri = new URIBuilder()
			.setScheme(webHdfsConfig.getScheme())
			.setHost(webHdfsConfig.getHost())
			.setPort(webHdfsConfig.getPort())
			.setPath(webHdfsConfig.getWEBHDFS_PREFIX()
				+ webHdfsConfig.getPath() + "/" 
				+ webHdfsConfig.getFileName())
			.setParameter("overwrite", webHdfsConfig.getOverwrite())
			.setParameter("user", webHdfsConfig.getUser())
			.build();
	}

	@Test
	public void validateCloseableHttpClient() throws URISyntaxException,
			UnsupportedEncodingException {
		//*************************************************
		// Send the request to the httpFS server, which 
		// should respond with a redirect (307) containing
		// the server and namenode to work with.
		//*************************************************
		StringEntity entity = new StringEntity("Greetings earthling!\n");
		HttpPut put = new HttpPut(uri);
		log.info (put.getURI().toString());
		put.setEntity(entity);
		log.info (put.getEntity().toString());

		CloseableHttpClient httpClient = HttpClients.createDefault(); 

		CloseableHttpResponse response = null;
		try {
			response = httpClient.execute(put);
			Assert.assertNotNull(response);
			log.info("Response status code {} ", response.getStatusLine().getStatusCode());
			Assert.assertEquals(307, response.getStatusLine().getStatusCode());
		} catch (IOException e) {
			throw new WebHdfsException("ERROR - failure to get redirect URL: "
					+ uri.toString(), e);
		}
		//*************************************************
		// Now get the redirect URL and write to HDFS.
		//*************************************************
		Header[] header = response.getHeaders("Location");
		Assert.assertNotNull(header);
		log.info(header[0].toString());
		String redirectUrl = header[0].toString().substring("Location:0".length());
		Assert.assertNotNull(redirectUrl);

		URI uri = new URIBuilder(redirectUrl)
			.setParameter("user", "spongecell")
			.build();

		HttpPut httpPut = new HttpPut(uri);
		httpPut.setEntity(entity);

		try {
			response = httpClient.execute(httpPut);
			Assert.assertEquals(201, response.getStatusLine().getStatusCode());
			httpClient.close();
			response.close();
		} catch (IOException e) {
			throw new WebHdfsException("ERROR - failure to write data to "
					+ uri.toString() + " Exception is: ", e);
		}
		log.info("Response status code {} ", response.getStatusLine().getStatusCode());
	}
	
	@Test
	public void validateWebHdfsCreate() throws URISyntaxException, UnsupportedEncodingException {
		Assert.assertNotNull(webHdfsBuilder);
		StringEntity entity = new StringEntity("Greetings earthling!\n");
		String user = "spongecell";
		String overwrite = "true";
		
		WebHdfs webHdfs = webHdfsBuilder
				.user(user)
				.entity(entity)
				.overwrite(overwrite)
				.build();
		Assert.assertNotNull(webHdfs);
		
		CloseableHttpResponse response = webHdfs.create(entity);
		Assert.assertNotNull(response);
		Assert.assertEquals(response.getStatusLine().getStatusCode(), 201);
	}

	
	@Test(dependsOnMethods="validateWebHdfsCreate")
	public void validateWebHdfsAppend() throws UnsupportedEncodingException,
			URISyntaxException {
		Assert.assertNotNull(webHdfsBuilder);
		StringEntity leaderEntity = new StringEntity("Take me to your leader!\n");
		StringEntity questionEntity = new StringEntity("What place is this?\n");
		
		WebHdfs webHdfs = webHdfsBuilder
				.user("spongecell")
				.entity(leaderEntity)
				.build();
		Assert.assertNotNull(webHdfs);
		
		CloseableHttpResponse response = webHdfs.append(leaderEntity);
		Assert.assertNotNull(response);
		Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
		
		response = webHdfs.append(questionEntity);
		Assert.assertNotNull(response);
		Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
	}	
}
