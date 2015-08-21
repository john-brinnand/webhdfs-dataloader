package webhdfs.dataloader.test;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;

import javax.annotation.PostConstruct;

import lombok.extern.slf4j.Slf4j;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import webhdfs.dataloader.WebHdfsConfiguration;
import webhdfs.dataloader.WebHdfsOps;
import webhdfs.dataloader.WebHdfsWorkFlow;

@Slf4j
@ContextConfiguration(classes = { WebHdfsWorkFlowTest.class, WebHdfsWorkFlow.Builder.class})
@EnableConfigurationProperties ({ WebHdfsConfiguration.class })
public class WebHdfsWorkFlowTest extends AbstractTestNGSpringContextTests{
	@Autowired WebHdfsWorkFlow.Builder webHdfsWorkFlowBuilder;

	@PostConstruct
	public void postConstruct() throws URISyntaxException {
		log.info("Placeholder.");
	}

	@Test
	public void validateCreateWorkFlow() throws NoSuchMethodException,
			SecurityException, UnsupportedEncodingException {
		Assert.assertNotNull(webHdfsWorkFlowBuilder);
		final String owner = "spongecell";
		final String group = "supergroup";
		final String dataDir = "/data";
		final String file = dataDir + "/testfile.txt";
		
		Object[] dataDirOwnerArgs = new Object[3];
		dataDirOwnerArgs[0] = dataDir;
		dataDirOwnerArgs[1] = owner;
		dataDirOwnerArgs[2] = group;	
		
		Object[] ownerArgs = new Object[3];
		ownerArgs[0] = file;
		ownerArgs[1] = owner;
		ownerArgs[2] = group;
		StringEntity createEntity = new StringEntity("Greetings earthling!\n");
		
		WebHdfsWorkFlow workFlow = webHdfsWorkFlowBuilder
			.addEntry(WebHdfsOps.MKDIRS, dataDir)
			.addEntry(WebHdfsOps.CREATE, createEntity)
			.addEntry(WebHdfsOps.SETOWNER, ownerArgs)
			.build();
		CloseableHttpResponse response = workFlow.execute();
		Assert.assertNotNull(response);
	}
}
