package webhdfs.dataloader.consumer;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedList;

import javax.annotation.PostConstruct;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import metamorphosis.kafka.consumer.MessageConsumer;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.util.Assert;

import webhdfs.dataloader.WebHdfs;
import webhdfs.dataloader.WebHdfsConfiguration;
import webhdfs.dataloader.WebHdfsOps;
import webhdfs.dataloader.WebHdfsOpsArgs;
import webhdfs.dataloader.WebHdfsWorkFlow;

@Slf4j
@Getter
@EnableConfigurationProperties ({ 
	WebHdfs.Builder.class, 
	WebHdfsConfiguration.class, 
	WebHdfsWorkFlow.Builder.class 
})
public class EventHandlerConsumer<K, V> implements MessageConsumer<K, V>  {
	private String topic;
	private int partition;
	private long offset;
	private K key;
	private V value;
	@Autowired WebHdfsWorkFlow.Builder webHdfsWorkFlowBuilder;
	@Autowired WebHdfsConfiguration webHdfsConfig;
	private WebHdfsWorkFlow workFlow;
	private boolean isCreated = false;
	
	@PostConstruct
	public void init() {
		LinkedList<Object> args = new LinkedList<Object>();
		args.add(0, webHdfsConfig.getBaseDir());
		args.add(1, null);
		args.add(2, webHdfsConfig.getBaseDir() + 
			"/" + webHdfsConfig.getFileName());
		args.add(3, webHdfsConfig.getOwner());
		args.add(4, webHdfsConfig.getGroup());
		args.add(5, webHdfsConfig.getBaseDir());
		args.add(6, webHdfsConfig.getOwner());
		args.add(7, webHdfsConfig.getGroup());
		
		WebHdfsOpsArgs mkdirOpArgs = new WebHdfsOpsArgs(
			WebHdfsOps.MKDIRS, args.subList(0, 1).toArray());
		WebHdfsOpsArgs createOpArgs = new WebHdfsOpsArgs(
			WebHdfsOps.CREATE, args.subList(1, 2).toArray());
		WebHdfsOpsArgs setFileOwnerOpArgs = new WebHdfsOpsArgs(
			WebHdfsOps.SETOWNER, args.subList(2, 5).toArray());
		WebHdfsOpsArgs setDirOwnerOpArgs = new WebHdfsOpsArgs(
			WebHdfsOps.SETOWNER, args.subList(5, args.size()).toArray());
		
		workFlow = webHdfsWorkFlowBuilder
			.addEntry("CreateBaseDir", mkdirOpArgs)
			.addEntry("SetBaseDirOwner", setDirOwnerOpArgs)
			.addEntry("CreateAndWriteToFile", createOpArgs)
			.addEntry("SetFileOwner", setFileOwnerOpArgs)
			.build();
	}

	@Override
	public void accept( String topic, int partition, long offset, K key, V value ) {
		this.topic = topic;
		this.partition = partition;
		this.offset = offset;
		this.key = key;
		this.value = value;
		if (key == null) {
			log.info("Empty message - returning.");
		}
		try {
			// This code could be part of a filter, which 
			// could be part of a pipeline.
			//**********************************************
			CloseableHttpResponse response = null; 
			String dataValue = value.toString().replace("\n", "").replace("\t", "") + "\n";
			StringEntity entity = new StringEntity(dataValue);
			if (isCreated == false) {
				ArrayList<StringEntity> entityList = new ArrayList<StringEntity>();
				entityList.add(entity);
			
				WebHdfsOpsArgs createOpArgs = new WebHdfsOpsArgs(WebHdfsOps.CREATE, entityList.toArray());	
				workFlow.getWorkFlow().put("CreateAndWriteToFile", createOpArgs);
			
				response = workFlow.execute();
				Assert.notNull(response, "Invalid response - value is null");
				isCreated = true;
			}
			else {
				response = workFlow.getWebHdfs().append(entity);
				Assert.notNull(response, "Invalid response - value is null");
			}
		} catch( UnsupportedEncodingException | URISyntaxException  e ) {
			throw new RuntimeException( "Error reading message", e );
		}
	}
}
