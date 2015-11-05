package webhdfs.dataloader.test;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.concurrent.atomic.AtomicLong;

import lombok.extern.slf4j.Slf4j;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import webhdfs.dataloader.FilePath;
import webhdfs.dataloader.WebHdfsConfiguration;
import webhdfs.dataloader.WebHdfsOps;
import webhdfs.dataloader.WebHdfsWorkFlow;

@Slf4j
@ContextConfiguration(classes = { WebHdfsWorkFlowTest.class, WebHdfsWorkFlow.Builder.class})
@EnableConfigurationProperties ({ WebHdfsConfiguration.class })
public class WebHdfsWorkFlowTest extends AbstractTestNGSpringContextTests{
	@Autowired WebHdfsWorkFlow.Builder webHdfsWorkFlowBuilder;
	@Autowired WebHdfsConfiguration webHdfsConfig;

	@Test
	public void validateWorkFlowOpsArgsConfiguration() throws NoSuchMethodException, 
		SecurityException, UnsupportedEncodingException, URISyntaxException {
		Assert.assertNotNull(webHdfsWorkFlowBuilder);
		
		StringEntity entity = new StringEntity("Greetings earthling!\n");
		
		DateTimeFormatter customDTF = new DateTimeFormatterBuilder()
	        .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
	        .appendValue(MONTH_OF_YEAR, 2)
	        .appendValue(DAY_OF_MONTH, 2)
	        .toFormatter();	
		
		FilePath path = new FilePath.Builder()
			.addPathSegment("data")
			.addPathSegment(customDTF.format(LocalDate.now()))
			.build();
		
		String fileName = path.getFile().getPath() + File.separator + webHdfsConfig.getFileName();
		
		WebHdfsWorkFlow workFlow = webHdfsWorkFlowBuilder
			.path(path.getFile().getPath())
			.addEntry("CreateBaseDir", 
				WebHdfsOps.MKDIRS, 
				HttpStatus.OK, 
				path.getFile().getPath())
			.addEntry("SetOwnerBaseDir",
				WebHdfsOps.SETOWNER, 
				HttpStatus.OK, 
				webHdfsConfig.getBaseDir(), 
				webHdfsConfig.getOwner(), 
				webHdfsConfig.getGroup())
			.addEntry("CreateAndWriteToFile", 
				WebHdfsOps.CREATE, 
				HttpStatus.CREATED, 
				entity)
			.addEntry("SetOwnerFile", WebHdfsOps.SETOWNER, 
				HttpStatus.OK, 
				fileName,
				webHdfsConfig.getOwner(), 
				webHdfsConfig.getGroup())
			.build();
		CloseableHttpResponse response = workFlow.execute(); 
		Assert.assertNotNull(response);
		Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.OK.value());
	}		
	@Test
	public void iterationTest () {
		AtomicLong time = new AtomicLong();
		Long next = time.incrementAndGet();
		log.info(":{}", String.format("%04d", next));
		next = time.incrementAndGet();
		log.info(":{}", String.format("%04d", next));
		next = time.incrementAndGet();
		log.info(":{}", String.format("%04d", next));
	}
}
