package webhdfs.dataloader.consumer;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;

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

@Slf4j
@Getter
@EnableConfigurationProperties ({ WebHdfs.Builder.class })
public class EventHandlerConsumer<K, V> implements MessageConsumer<K, V>  {
	private String topic;
	private int partition;
	private long offset;
	private K key;
	private V value;
	@Autowired WebHdfs.Builder webHdfsBuilder;
	private WebHdfs webHdfs;
	private boolean isCreated = false;
	
	@PostConstruct
	public void init() {
		log.info("Here");
		webHdfs = webHdfsBuilder
			.user("spongecell")
			.overwrite("true")
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
			String dataValue = value.toString().replace("\n", "").replace("\t", "") + "\n";
			StringEntity entity = new StringEntity(dataValue);
			
			CloseableHttpResponse response = null; 
			if (isCreated == false) {
				response = webHdfs.create(entity);
				Assert.notNull(response, "Invalid response - value is null");
				isCreated = true;
			}
			else {
				response = webHdfs.append(entity);
				Assert.notNull(response, "Invalid response - value is null");
			}
		} catch( UnsupportedEncodingException | URISyntaxException  e ) {
			throw new RuntimeException( "Error reading message", e );
		}
	}
}
