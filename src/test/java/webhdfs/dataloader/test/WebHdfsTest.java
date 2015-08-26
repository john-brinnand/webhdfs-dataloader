package webhdfs.dataloader.test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.PostConstruct;

import lombok.extern.slf4j.Slf4j;

import org.apache.http.Header;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import webhdfs.dataloader.WebHdfs;
import webhdfs.dataloader.WebHdfsConfiguration;
import webhdfs.dataloader.WebHdfsParams;
import webhdfs.dataloader.exception.WebHdfsException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static webhdfs.dataloader.WebHdfsParams.*;

/**
 * @author jbrinnand
 */
@Slf4j
@ContextConfiguration(classes = { WebHdfsTest.class, WebHdfs.Builder.class})
@EnableConfigurationProperties ({ WebHdfsConfiguration.class })
public class WebHdfsTest extends AbstractTestNGSpringContextTests{
	@Autowired WebHdfsConfiguration webHdfsConfig;
	@Autowired WebHdfs.Builder webHdfsBuilder;
	private URI uri;
	private StringEntity greetingEntity = null; 
	private StringEntity leaderEntity =  null;
	private StringEntity questionEntity = null; 

	@PostConstruct
	public void postConstruct() throws URISyntaxException, UnsupportedEncodingException {
		uri = new URIBuilder()
			.setScheme(webHdfsConfig.getScheme())
			.setHost(webHdfsConfig.getHost())
			.setPort(webHdfsConfig.getPort())
			.setPath(webHdfsConfig.getWEBHDFS_PREFIX()
				+ webHdfsConfig.getPath() + "/" 
				+ webHdfsConfig.getFileName())
			.setParameter(OWNER, webHdfsConfig.getSuperUser())
			.setParameter(OVERWRITE, webHdfsConfig.getOverwrite())
			.setParameter(USER, webHdfsConfig.getUser())
			.setParameter(OP, WebHdfsParams.CREATE)
			.build();
		
		greetingEntity = new StringEntity("Greetings earthling!\n");
		questionEntity = new StringEntity("What place is this?\n");
		leaderEntity = new StringEntity("Take me to your leader!\n");
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
			Assert.assertEquals(response.getStatusLine().getStatusCode(), 307);
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

		uri = new URIBuilder(redirectUrl)
			.addParameter(USER, webHdfsConfig.getUser())
			.addParameter(PERMISSION, DEFAULT_PERMISSIONS)
			.build();

		HttpPut httpPut = new HttpPut(uri);
		httpPut.setEntity(entity);

		try {
			response = httpClient.execute(httpPut);
			Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.CREATED.value());
			httpClient.close();
			response.close();
		} catch (IOException e) {
			throw new WebHdfsException("ERROR - failure to write data to "
					+ uri.toString() + " Exception is: ", e);
		}
		log.info("Response status code {} ", response.getStatusLine().getStatusCode());
	}
	
	@Test(dependsOnMethods="validateWebHdfsMkdirs")
	public void validateWebHdfsCreate() throws URISyntaxException, UnsupportedEncodingException {
		Assert.assertNotNull(webHdfsBuilder);
		final String overwrite = webHdfsConfig.getOverwrite();
		final String superUser =  webHdfsConfig.getSuperUser();
		
		WebHdfs webHdfs = webHdfsBuilder
				.user(superUser)
				.overwrite(overwrite)
				.build();
		Assert.assertNotNull(webHdfs);
		
		CloseableHttpResponse response = webHdfs.create(greetingEntity);
		Assert.assertNotNull(response);
		Assert.assertEquals(response.getStatusLine().getStatusCode(), 201);
	}
	
	@Test(dependsOnMethods="validateWebHdfsCreate")
	public void validateWebHdfsAppend() throws UnsupportedEncodingException,
			URISyntaxException {
		Assert.assertNotNull(webHdfsBuilder);
		final String superUser =  webHdfsConfig.getSuperUser();
		
		WebHdfs webHdfs = webHdfsBuilder
				.user(superUser)
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
		final WebHdfs webHdfs = webHdfsBuilder.build();
		Assert.assertNotNull(webHdfs);
		final String superUser =  webHdfsConfig.getSuperUser();
		
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
		
		Assert.assertEquals(fileStatus.get("FileStatus").get(TYPE).asText(),
				FILE);
		Assert.assertEquals(fileStatus.get("FileStatus")
			.get(PERMISSION).asText(), DEFAULT_PERMISSIONS);
		Assert.assertEquals(fileStatus.get("FileStatus").get(OWNER).asText(), superUser);
	}	
	
	@Test(dependsOnMethods="validateWebHdfsCreate")
	public void validateWebHdfsListStatus() throws URISyntaxException, IOException {
		Assert.assertNotNull(webHdfsBuilder);
		String dataDir = webHdfsConfig.getBaseDir(); 
		final String superUser =  webHdfsConfig.getSuperUser();
		
		WebHdfs webHdfs = webHdfsBuilder.build();
		Assert.assertNotNull(webHdfs);
		
		
		CloseableHttpResponse response = null;
		try {
			response = webHdfs.listStatus(dataDir);
		} catch (IllegalArgumentException e) {
			log.info (e.getCause().toString());
		}
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
			.get(FILE_STATUSES)
			.get(FILE_STATUS).toString(),
			new TypeReference<ArrayNode>() { 
		});
		
		for (int i = 0; i < fileStatus.size(); i++) {
			JsonNode fileStatusNode = fileStatus.get(i);
			Assert.assertEquals(fileStatusNode.get(TYPE).asText(), FILE);
			Assert.assertEquals(fileStatusNode.get(PERMISSION).asText(), DEFAULT_PERMISSIONS);
			Assert.assertEquals(fileStatusNode.get(OWNER).asText(), superUser);	
		}
	}
	
	@Test(dependsOnMethods="validateWebHdfsCreate")
	public void validateWebHdfsGetContentSummary() throws URISyntaxException, IOException {
		Assert.assertNotNull(webHdfsBuilder);
		
		WebHdfs webHdfs = webHdfsBuilder.build();
		Assert.assertNotNull(webHdfs);
		
		CloseableHttpResponse response = webHdfs.getContentSummary(webHdfsConfig.getFileName());
		Assert.assertNotNull(response);
		Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
		
		//****************************************
		ObjectNode contentSummary = new ObjectMapper().readValue(
			EntityUtils.toString(response.getEntity()), 
			new TypeReference<ObjectNode>() {
		});
		log.info("File status is: {} ", new ObjectMapper()
			.writerWithDefaultPrettyPrinter()
			.writeValueAsString(contentSummary));
			
		Assert.assertNotNull(contentSummary.get(CONTENT_SUMMARY).get(DIRECTORY_COUNT));
	}

	@Test
	public void validateWebHdfsMkdirs() throws URISyntaxException, UnsupportedEncodingException {
		Assert.assertNotNull(webHdfsBuilder);
		final String user =  webHdfsConfig.getSuperUser();
		final String overwrite = webHdfsConfig.getOverwrite();
		final String dataDir = webHdfsConfig.getBaseDir();
		
		WebHdfs webHdfs = webHdfsBuilder
				.user(user)
				.overwrite(overwrite)
				.build();
		Assert.assertNotNull(webHdfs);
		
		CloseableHttpResponse response = webHdfs.listStatus(dataDir);
		if (response.getStatusLine().getStatusCode() == HttpStatus.NOT_FOUND.value()) {
			response = webHdfs.mkdirs(dataDir);
		}
		Assert.assertNotNull(response);
		Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
	}
	
	@Test(dependsOnMethods="validateWebHdfsCreate")
	public void validateWebHdfsSetOwner() throws URISyntaxException,
			JsonParseException, JsonMappingException, ParseException, IOException {
		Assert.assertNotNull(webHdfsBuilder);
		final String user =  webHdfsConfig.getSuperUser();
		final String owner = webHdfsConfig.getOwner(); 
		final String group = webHdfsConfig.getGroup(); 
		final String overwrite = webHdfsConfig.getOverwrite();
		final String file = webHdfsConfig.getBaseDir() + "/" + webHdfsConfig.getFileName(); 
		
		WebHdfs webHdfs = webHdfsBuilder
				.user(user)
				.overwrite(overwrite)
				.build();
		Assert.assertNotNull(webHdfs);
		
		CloseableHttpResponse response = webHdfs.setOwner(file, owner, group);
		Assert.assertNotNull(response);
		Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.OK.value());
	}
}
