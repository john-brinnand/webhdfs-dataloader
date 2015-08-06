package datavalidator.test;

import lombok.Getter;
import lombok.Setter;

import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter @Setter
@ConfigurationProperties(prefix ="webhdfs")
public class WebHdfsConfiguration {
	public String scheme = "http";
	public String fileName = "testfile.txt";
	public int port = 50070;
	public String path = "/data";
	public String host = "dockerhadoop";
	public String WEBHDFS_PREFIX = "/webhdfs/v1";
	
}
