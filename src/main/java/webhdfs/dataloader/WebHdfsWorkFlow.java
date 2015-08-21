package webhdfs.dataloader;

import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.StringEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@Slf4j
public class WebHdfsWorkFlow {
	private Map<WebHdfsOps, Object[]> workflow;
	private Map<String, WebHdfsOpsArgs> workFlow;
	private WebHdfs webHdfs;
	
	private WebHdfsWorkFlow(Builder builder) {
		this.workflow = builder.workflow;
		this.workFlow = builder.workFlow;
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
		private Map<String, WebHdfsOpsArgs> workFlow;

		public Builder() {
			workflow = new LinkedHashMap<WebHdfsOps, Object[]>();
			workFlow = new LinkedHashMap<String, WebHdfsOpsArgs>();
		}
		
		public Builder addEntry(WebHdfsOps op, Object...args) {
			log.info("WorkFlow entry is: {} ", op);
			workflow.put(op, args);
			return this;
		}
		public Builder addEntry(String step, WebHdfsOpsArgs opsArgs) {
			this.workFlow.put(step, opsArgs);
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
	public CloseableHttpResponse execute() throws URISyntaxException {
		CloseableHttpResponse response = null;
		Set<Entry<String, WebHdfsOpsArgs>>entries = workFlow.entrySet();
		for (Entry<String, WebHdfsOpsArgs>entry : entries) {
			WebHdfsOpsArgs opsArgs = entry.getValue();
			log.info("Executing step : {} ", entry.getKey());
			if (opsArgs.getWebHdfsOp().equals(WebHdfsOps.LISTSTATUS)) {
				response = webHdfs.listStatus((String)opsArgs.getArgs()[0]);
				continue;
			}
			if (opsArgs.getWebHdfsOp().equals(WebHdfsOps.GETFILESTATUS)) {
				response = webHdfs.getFileStatus((String)opsArgs.getArgs()[0]);
				continue;
			}					
			if (opsArgs.getWebHdfsOp().equals(WebHdfsOps.CREATE)) {
				response = webHdfs.create((AbstractHttpEntity)opsArgs.getArgs()[0]);
				continue;
			}
			if (opsArgs.getWebHdfsOp().equals(WebHdfsOps.APPEND)) {
				response = webHdfs.append((StringEntity)opsArgs.getArgs()[0]);
				continue;
			}	
			if (opsArgs.getWebHdfsOp().equals(WebHdfsOps.MKDIRS)) {
				response = webHdfs.mkdirs((String)opsArgs.getArgs()[0]);
				continue;
			}		
			if (opsArgs.getWebHdfsOp().equals(WebHdfsOps.SETOWNER)) {
				response = webHdfs.setOwner((String)opsArgs.getArgs()[0], 
						(String)opsArgs.getArgs()[1], 
						(String)opsArgs.getArgs()[2]); 
				continue;
			}					
		}
		return response;
	}	
		
}
