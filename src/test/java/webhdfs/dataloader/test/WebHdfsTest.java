package webhdfs.dataloader.test;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import javax.annotation.PostConstruct;

import lombok.extern.slf4j.Slf4j;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.util.JSONWrappedObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import webhdfs.dataloader.WebHdfs;
import webhdfs.dataloader.WebHdfsConfiguration;
import webhdfs.dataloader.exception.WebHdfsException;

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

		URI uri = new URIBuilder(redirectUrl).build();

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
				.build();
		Assert.assertNotNull(webHdfs);
		
		CloseableHttpResponse response = webHdfs.append(questionEntity);
		Assert.assertNotNull(response);
		Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
		
		response = webHdfs.append(leaderEntity);
		Assert.assertNotNull(response);
		Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
	}	
	
	@Test(dependsOnMethods="validateWebHdfsCreate")
	public void validateWebHdfsFileStatus() throws URISyntaxException, IOException {
		Assert.assertNotNull(webHdfsBuilder);
		
		WebHdfs webHdfs = webHdfsBuilder.build();
		Assert.assertNotNull(webHdfs);
		
		CloseableHttpResponse response = webHdfs.getFileStatus(webHdfsConfig.getFileName());
		Assert.assertNotNull(response);
		Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
		
		ObjectNode fileStatus = new ObjectMapper().readValue(
			EntityUtils.toString(response.getEntity()), 
			new TypeReference<ObjectNode>() {
		});
		log.info("File status is: {} ", new ObjectMapper()
			.writerWithDefaultPrettyPrinter()
			.writeValueAsString(fileStatus));
		
		Assert.assertEquals(fileStatus.get("FileStatus").get("type").asText(), "FILE");
		Assert.assertEquals(fileStatus.get("FileStatus").get("permission").asText(), "755");
		Assert.assertEquals(fileStatus.get("FileStatus").get("owner").asText(), "dr.who");
	}	
	
	@Test(dependsOnMethods="validateWebHdfsCreate")
	public void validateWebHdfsListStatus() throws URISyntaxException, IOException {
		Assert.assertNotNull(webHdfsBuilder);
		String dataDir = "/data";
		
		WebHdfs webHdfs = webHdfsBuilder.build();
		Assert.assertNotNull(webHdfs);
		
		CloseableHttpResponse response = webHdfs.listStatus(dataDir);
		Assert.assertNotNull(response);
		Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
		
		ObjectNode dirStatus = new ObjectMapper().readValue(
			EntityUtils.toString(response.getEntity()), 
			new TypeReference<ObjectNode>() {
		});
		log.info("Directory status is: {} ", new ObjectMapper()
			.writerWithDefaultPrettyPrinter()
			.writeValueAsString(dirStatus));
		
		ArrayNode fileStatus  = new ObjectMapper().readValue(dirStatus
			.get("FileStatuses")
			.get("FileStatus").toString(),
			new TypeReference<ArrayNode>() { 
		});
		
		for (int i = 0; i < fileStatus.size(); i++) {
			JsonNode fileStatusNode = fileStatus.get(i);
			Assert.assertEquals(fileStatusNode.get("type").asText(), "FILE");
			Assert.assertEquals(fileStatusNode.get("permission").asText(), "755");
			Assert.assertEquals(fileStatusNode.get("owner").asText(), "dr.who");	
		}
	}
}
