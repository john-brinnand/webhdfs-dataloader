package webhdfs.dataloader.consumer;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import metamorphosis.kafka.consumer.MessageConsumer;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.StringEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.HttpStatus;
import org.springframework.util.Assert;

import webhdfs.dataloader.FilePath;
import webhdfs.dataloader.WebHdfs;
import webhdfs.dataloader.WebHdfsConfiguration;
import webhdfs.dataloader.WebHdfsOps;
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
	private DateTimeFormatter customDTF;
	private static AtomicLong iteration; 
	
	@PostConstruct
	public void init() {
		customDTF = new DateTimeFormatterBuilder()
	        .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
	        .appendValue(MONTH_OF_YEAR, 2)
	        .appendValue(DAY_OF_MONTH, 2)
	        .toFormatter();
		
		iteration = new AtomicLong();
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
			
				WebHdfsWorkFlow workFlow = buildWorkFlow(entity);
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
	
	private WebHdfsWorkFlow buildWorkFlow (AbstractHttpEntity entity) {
		Long nextIteration = iteration.incrementAndGet();
		log.info("nextIteration is: {}", nextIteration);
		FilePath baseDir = new FilePath.Builder()
			.addPathSegment(webHdfsConfig.getBaseDir())
			.addPathSegment(customDTF.format(LocalDate.now()) + 
				String.format("%04d", nextIteration))
			.build();
		
		FilePath fileName = new FilePath.Builder()
			.addPathSegment(baseDir.getFile().getPath())
			.addPathSegment(webHdfsConfig.getFileName())
			.build();
		
		workFlow = webHdfsWorkFlowBuilder
			.path(baseDir.getFile().getPath())
			.addEntry("CreateBaseDir", 
				WebHdfsOps.MKDIRS, 
				HttpStatus.OK, 
				baseDir.getFile().getPath())
			.addEntry("SetBaseDirOwner", 
				WebHdfsOps.SETOWNER, 
				HttpStatus.OK, 
				webHdfsConfig.getBaseDir(), 
				webHdfsConfig.getOwner(), 
				webHdfsConfig.getGroup())
			.addEntry("CreateAndWriteToFile", 
				WebHdfsOps.CREATE, 
				HttpStatus.CREATED, 
				entity)
			.addEntry("SetFileOwner", WebHdfsOps.SETOWNER, 
				HttpStatus.OK, 
				fileName.getFile().getPath(),
				webHdfsConfig.getOwner(), 
				webHdfsConfig.getGroup())
			.build();		
		return workFlow;
	}

	public void setCreated(boolean isCreated) {
		this.isCreated = isCreated;
	}
}
