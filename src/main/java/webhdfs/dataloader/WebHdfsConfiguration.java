package webhdfs.dataloader;

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
	public String baseDir = "/data";
	public String host = "dockerhadoop";
	public String user = "root";
	public String superUser = "root";
	public String owner = "spongecell";
	public String group = "supergroup";
	public String overwrite = "true";
	public String WEBHDFS_PREFIX = "/webhdfs/v1";
	
}
