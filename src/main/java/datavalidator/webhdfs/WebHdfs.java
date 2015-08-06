package datavalidator.webhdfs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(classes = { WebHdfs.class })
@EnableConfigurationProperties ({ WebHdfsConfiguration.class })
public class WebHdfs {
	@Autowired WebHdfsConfiguration webHdfsConfig;
	private String host ;
	private String path;
	private String fileName;
	private final int port = 50070;
	
	public WebHdfs () {}
	
	private WebHdfs (Builder builder) {
		host = builder.host;
		path = builder.path;
		fileName = builder.fileName;
	}

	public static class Builder {
		private String host;
		private String path;
		private String fileName;
		
		public Builder host(String host) {
			this.host = host;
			return this;
		}
		
		public Builder path(String path) {
			this.path = path;
			return this;
		}
		
		public Builder fileName(String fileName) {
			this.fileName = fileName;
			return this;
		}	
		
		public WebHdfs build () {
			return new WebHdfs(this);
		}
	}
}
