package webhdfs.dataloader;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import lombok.extern.slf4j.Slf4j;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.util.Assert;

import webhdfs.dataloader.exception.WebHdfsException;

import static webhdfs.dataloader.WebHdfsParams.*;


@Slf4j
public class WebHdfs {
	private WebHdfsConfiguration webHdfsConfig;
	private CloseableHttpClient httpClient;
	private Map<String, URI> uriMap; 
	
	public WebHdfs () {}
	
	private WebHdfs (Builder builder) {
		webHdfsConfig = builder.webHdfsConfig;
		
		if (builder.fileName != null)  {
			webHdfsConfig.setFileName(builder.fileName);
		}
		if (builder.overwrite != null) {
			webHdfsConfig.setOverwrite(builder.overwrite);
		}
		if (builder.user != null) {
			webHdfsConfig.setUser(builder.user);
		}
		httpClient = HttpClients.createDefault();
		uriMap = new HashMap<String, URI>();
	}

	@EnableConfigurationProperties ({ WebHdfsConfiguration.class })
	public static class Builder {
		@Autowired WebHdfsConfiguration webHdfsConfig;
		private String fileName;
		private String user;
		private String overwrite;
		
		public Builder () {
			this.overwrite = "true";
		}
		
		public Builder fileName(String fileName) {
			this.fileName = fileName;
			return this;
		}	
		
		public Builder user(String user) {
			this.user = user;
			return this;
		}	
		
		public Builder overwrite(String overwrite) {
			this.overwrite = overwrite;
			return this;
		}				
		
		public WebHdfs build () {
			return new WebHdfs(this);
		}
	}
	
	/**
	 * Create the file in HDFS. 
	 * 
	 * @throws URISyntaxException 
	 */
	public CloseableHttpResponse create (final AbstractHttpEntity entity) throws WebHdfsException {
		URI uri = null;
		try {
			uri = new URIBuilder()
				.setScheme(webHdfsConfig.getScheme())
				.setHost(webHdfsConfig.getHost())
				.setPort(webHdfsConfig.getPort())
				.setPath(webHdfsConfig.getWEBHDFS_PREFIX()
					+ webHdfsConfig.getPath() + "/" 
					+ webHdfsConfig.getFileName())
				.setParameter("overwrite", webHdfsConfig.getOverwrite())
				.setParameter(USER, webHdfsConfig.getUser())
				.setParameter(OP, CREATE)
				.build();
		} catch (URISyntaxException e) {
			throw new WebHdfsException("ERROR - failure to create URI. Cause is:  ", e);	
		}
		
		HttpPut put = new HttpPut(uri);
		log.info ("URI is : {} ", put.getURI().toString());
		
		put.setEntity(entity);
		
		CloseableHttpResponse response = null;
		try {
			log.info ("Entity is : {} ", EntityUtils.toString(put.getEntity()));
			response = httpClient.execute(put);
			Assert.notNull(response);
			log.info("Response status code {} ", response.getStatusLine().getStatusCode());
			Assert.isTrue(response.getStatusLine().getStatusCode() == 307, 
				"Response code indicates a failed write");	
			
			response = write(response, put, HttpServletResponse.SC_CREATED, entity);
			response.close();
			
		} catch (IOException e) {
			throw new WebHdfsException("ERROR - failure to create file: "
					+ uri.toString(), e);
		}	
		finally {
			put.completed();
		}
		return response;
	}
	
	public CloseableHttpResponse append (final StringEntity entity) throws URISyntaxException {
		HttpPost httpPost = null;
		URI uri = null;
		if ((uri = uriMap.get("appendURI")) != null) {
			httpPost = new HttpPost(uri);
		}
		else {
			uri = new URIBuilder()
				.setScheme(webHdfsConfig.getScheme())
				.setHost(webHdfsConfig.getHost())
				.setPort(webHdfsConfig.getPort())
				.setPath(webHdfsConfig.getWEBHDFS_PREFIX()
					+ webHdfsConfig.getPath() + "/" 
					+ webHdfsConfig.getFileName())
				.setParameter("overwrite", webHdfsConfig.getOverwrite())
				.setParameter(USER, webHdfsConfig.getUser())
				.setParameter(OP, APPEND)	
				.build();
			uriMap.put("appendURI", uri);
			httpPost = new HttpPost(uriMap.get("appendURI"));
		}
		log.info ("URI is : {} ", httpPost.getURI().toString());
		
		CloseableHttpResponse response = null;
		try {
			// Append appears to require a new HTTP
			// client for every operation.
			//**************************************
			CloseableHttpClient client = HttpClients.createDefault();
			response = client.execute(httpPost);
			Assert.notNull(response);
			Assert.isTrue(response.getStatusLine().getStatusCode() == 307, 
					"Response code indicates a failed write");	
			response = write(response, httpPost, HttpServletResponse.SC_OK, entity);
			
			// Closes all resources.
			//**********************
			response.close();
		} catch (IOException e) {
			throw new WebHdfsException("ERROR - failure to get redirect URL: "
					+ uriMap.get("appendURI").toString(), e);
		}	
		httpPost.completed();
		return response;
	}	
	
	/**
	 * Retrieves the file status.
	 * 
	 * @param fileName
	 * @return
	 * @throws WebHdfsException
	 */
	public CloseableHttpResponse getFileStatus (String fileName) throws WebHdfsException {
		CloseableHttpResponse response = null;
		try {
			final URI uri = new URIBuilder()
				.setScheme(webHdfsConfig.getScheme())
				.setHost(webHdfsConfig.getHost())
				.setPort(webHdfsConfig.getPort())
				.setPath(webHdfsConfig.getWEBHDFS_PREFIX()
						+ webHdfsConfig.getPath() + "/" 
						+ webHdfsConfig.getFileName())
				.setParameter("overwrite", "false")
				.setParameter(USER, webHdfsConfig.getUser())
				.setParameter(OP, GETFILESTATUS)				
				.build();
			
			HttpGet httpMethod = new HttpGet(uri);
			response = httpClient.execute(httpMethod);
			Assert.notNull(response);
			Assert.isTrue(response.getStatusLine().getStatusCode() == 200, 
					"Response code indicates a failed write");	
			
		} catch (URISyntaxException | IOException e) {
			log.error("ERROR -  ");
		}
		log.info("Returning http response. Status code is: {}", 
			response.getStatusLine().getStatusCode());
		return response;
	}
	
	/**
	 * This method gets the directory listing. This means it will return 
	 * the current listing and all child directories and files below it.
	 * 
	 * @param fileName
	 * @return
	 * @throws WebHdfsException
	 */
	public CloseableHttpResponse listStatus (String fileName) throws WebHdfsException {
		CloseableHttpResponse response = null;

		if (!fileName.equals("/data")) {
			String tempFileName = webHdfsConfig.getWEBHDFS_PREFIX()
					+ webHdfsConfig.getPath() + "/" 
					+ fileName;			
			fileName = tempFileName;
		}
		else {
			String tempFileName = webHdfsConfig.getWEBHDFS_PREFIX()
					+ webHdfsConfig.getPath();
			fileName = tempFileName;			
		}
		try {
			final URI uri = new URIBuilder()
				.setScheme(webHdfsConfig.getScheme())
				.setHost(webHdfsConfig.getHost())
				.setPort(webHdfsConfig.getPort())
				.setPath(fileName)
				.setParameter("overwrite", "false")
				.setParameter(USER, webHdfsConfig.getUser())
				.setParameter(OP, LISTSTATUS)	
				.build();
			
			HttpGet httpMethod = new HttpGet(uri);
			response = httpClient.execute(httpMethod);
			Assert.notNull(response);
			Assert.isTrue(response.getStatusLine().getStatusCode() == 200, 
					"Response code indicates a failed write");	
			
		} catch (URISyntaxException | IOException e) {
			log.error("ERROR -  ");
		}
		log.info("Returning http response. Status code is: {}", 
			response.getStatusLine().getStatusCode());
		return response;
	}
	
	public CloseableHttpResponse getContentSummary (String fileName) {
		CloseableHttpResponse response = null;
		try {
			final URI uri = new URIBuilder()
			.setScheme(webHdfsConfig.getScheme())
			.setHost(webHdfsConfig.getHost())
			.setPort(webHdfsConfig.getPort())
			.setPath(webHdfsConfig.getWEBHDFS_PREFIX()
					+ webHdfsConfig.getPath() + "/" 
					+ fileName)
			.setParameter("overwrite", "false")
			.setParameter(USER, webHdfsConfig.getUser())
			.setParameter(OP, GETCONTENTSUMMARY)
			.build();

			HttpGet httpMethod = new HttpGet(uri);
			response = httpClient.execute(httpMethod);
			Assert.notNull(response);
			Assert.isTrue(response.getStatusLine().getStatusCode() == 200, 
					"Response code indicates a failed write");	

		} catch (URISyntaxException | IOException e) {
			log.error("ERROR -  ");
		}
		log.info("Returning http response. Status code is: {}", 
				response.getStatusLine().getStatusCode());
		return response;
	}
	
	private CloseableHttpResponse write(CloseableHttpResponse response, 
			HttpEntityEnclosingRequestBase httpRequest, int responseCode, AbstractHttpEntity entity) {
		//*************************************************
		// Now get the redirect URL and write to HDFS.
		//*************************************************
		Header[] header = response.getHeaders("Location");
		Assert.notNull(header);
		log.debug(header[0].toString());
		String redirectUrl = header[0].toString().substring("Location:0".length());

		URI uri = null; 
		try {
			uri = new URIBuilder(redirectUrl).build();
			
			httpRequest.setURI(uri);
			httpRequest.setEntity(entity);
			log.debug ("Entity is : {} ", EntityUtils.toString(httpRequest.getEntity()));
			
			response = httpClient.execute(httpRequest);
			log.debug("Response status code {} ", response.getStatusLine().getStatusCode());
			Assert.isTrue(response.getStatusLine().getStatusCode() == responseCode, 
				"Response code indicates a failed write");
			
		} catch (IOException | URISyntaxException e) {
			throw new WebHdfsException("ERROR - failure to build URI or write data." + 
				 " Exception is: ", e);
		}
		return response;
	}	
}
