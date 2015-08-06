package datavalidator.webhdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import lombok.extern.slf4j.Slf4j;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.msgpack.core.Preconditions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.Assert;

import datavalidator.webhdfs.exception.WebHdfsException;

@Slf4j
public class WebHdfs {
	private String fileName;
	private StringEntity entity;
	private URI uri;
	private String user;
	private WebHdfsConfiguration webHdfsConfig;
	private CloseableHttpClient httpClient;
	
	public WebHdfs () {}
	
	private WebHdfs (Builder builder) {
		fileName = builder.fileName;
		entity = builder.entity;
		user = builder.user;
		webHdfsConfig = builder.webHdfsConfig;
		
		try {
			uri = new URIBuilder()
				.setScheme(webHdfsConfig.getScheme())
				.setHost(webHdfsConfig.getHost())
				.setPort(webHdfsConfig.getPort())
				.setPath(webHdfsConfig.getWEBHDFS_PREFIX()
						+ webHdfsConfig.getPath() + "/" 
						+ webHdfsConfig.getFileName())
				.setParameter("overwrite", "true")
				.setParameter("op", "CREATE")
				.setParameter("user", user)
				.build();
			
			httpClient = HttpClients.createDefault(); 
		} catch (URISyntaxException e) {
			throw new WebHdfsException("ERROR failed to create URI", e);
		}
	}

	@ContextConfiguration(classes = { WebHdfs.Builder.class })
	@EnableConfigurationProperties ({ WebHdfsConfiguration.class })
	public static class Builder {
		@Autowired WebHdfsConfiguration webHdfsConfig;
		private String fileName;
		private StringEntity entity;
		private String user;
		
		public Builder fileName(String fileName) {
			this.fileName = fileName;
			return this;
		}	
		
		public Builder entity(StringEntity entity) {
			this.entity = entity;
			return this;
		}
		
		public Builder user(String user) {
			this.user = user;
			return this;
		}	
			
		public WebHdfs build () {
			return new WebHdfs(this);
		}
	}
	
	/**
	 * Create the target file. 
	 */
	public CloseableHttpResponse create () {
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
			
			response = write(response);
			
		} catch (IOException e) {
			throw new WebHdfsException("ERROR - failure to get redirect URL: "
					+ uri.toString(), e);
		}	
		put.completed();
		
		return response;
	}
	
	private CloseableHttpResponse write(CloseableHttpResponse response) {
		//*************************************************
		// Now get the redirect URL and write to HDFS.
		//*************************************************
		Header[] header = response.getHeaders("Location");
		Assert.notNull(header);
		log.info(header[0].toString());
		String redirectUrl = header[0].toString().substring("Location:0".length());

		try {
			URI uri = new URIBuilder(redirectUrl)
				.setParameter("user", "spongecell")
				.build();

			HttpPut httpPut = new HttpPut(uri);
			httpPut.setEntity(entity);
			
			response = httpClient.execute(httpPut);
			log.info("Response status code {} ", response.getStatusLine().getStatusCode());
			Assert.isTrue(response.getStatusLine().getStatusCode() == 201, 
				"Response code indicates a failed write");
			
			httpClient.close();
			response.close();
		} catch (IOException | URISyntaxException e) {
			throw new WebHdfsException("ERROR - failure to write data to "
					+ uri.toString() + " Exception is: ", e);
		}
		return response;
	}
}
