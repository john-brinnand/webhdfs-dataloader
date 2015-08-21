package webhdfs.dataloader;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.AbstractHttpEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@Slf4j
public class WebHdfsWorkFlow {
	private Map<WebHdfsOps, Object[]> workflow;
	private WebHdfs webHdfs;
	
	private WebHdfsWorkFlow(Builder builder) {
		this.workflow = builder.workflow;
		webHdfs = builder.webHdfsBuilder
			.fileName(builder.fileName)
			.user(builder.user)
			.overwrite(builder.overwrite)
			.build();
	}

	@EnableConfigurationProperties ({ WebHdfs.Builder.class })
	public static class Builder {
		@Autowired WebHdfs.Builder webHdfsBuilder;
		private Map<WebHdfsOps, Object[]> workflow;
		private String fileName;
		private String user;
		private String overwrite;

		public Builder() {
			workflow = new LinkedHashMap<WebHdfsOps, Object[]>();
		}
		
		public Builder addEntry(WebHdfsOps op, Object...args) throws NoSuchMethodException, SecurityException {
			log.info("WorkFlow entry is: {} ", op);
			workflow.put(op, args);
			return this;
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
		
		public WebHdfsWorkFlow build() {
			return new WebHdfsWorkFlow(this);
		}
	}
	public CloseableHttpResponse execute() {
		CloseableHttpResponse response = null;
		Set<Entry<WebHdfsOps, Object[]>>entries = workflow.entrySet();
		for (Entry<WebHdfsOps, Object[]>entry : entries) {
			if (entry.getKey().equals(WebHdfsOps.LISTSTATUS)) {
				response = webHdfs.listStatus((String)entry.getValue()[0]);
				continue;
			}
			if (entry.getKey().equals(WebHdfsOps.GETFILESTATUS)) {
				response = webHdfs.getFileStatus((String)entry.getValue()[0]);
				continue;
			}					
			if (entry.getKey().equals(WebHdfsOps.CREATE)) {
				response = webHdfs.create((AbstractHttpEntity)entry.getValue()[0]);
				continue;
			}
			if (entry.getKey().equals(WebHdfsOps.APPEND)) {
				response = webHdfs.create((AbstractHttpEntity)entry.getValue()[0]);
				continue;
			}	
			if (entry.getKey().equals(WebHdfsOps.MKDIRS)) {
				response = webHdfs.mkdirs((String)entry.getValue()[0]);
				continue;
			}		
			if (entry.getKey().equals(WebHdfsOps.SETOWNER)) {
				response = webHdfs.setOwner((String)entry.getValue()[0], 
						(String)entry.getValue()[1], 
						(String)entry.getValue()[2]);
				continue;
			}					
		}
		return response;
	}
}
