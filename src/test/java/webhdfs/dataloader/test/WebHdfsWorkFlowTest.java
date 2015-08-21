package webhdfs.dataloader.test;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.LinkedList;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import webhdfs.dataloader.WebHdfsConfiguration;
import webhdfs.dataloader.WebHdfsOps;
import webhdfs.dataloader.WebHdfsOpsArgs;
import webhdfs.dataloader.WebHdfsWorkFlow;

@ContextConfiguration(classes = { WebHdfsWorkFlowTest.class, WebHdfsWorkFlow.Builder.class})
@EnableConfigurationProperties ({ WebHdfsConfiguration.class })
public class WebHdfsWorkFlowTest extends AbstractTestNGSpringContextTests{
	@Autowired WebHdfsWorkFlow.Builder webHdfsWorkFlowBuilder;

	@Test
	public void validateCreateWorkFlow() throws NoSuchMethodException,
			SecurityException, UnsupportedEncodingException, URISyntaxException {
		Assert.assertNotNull(webHdfsWorkFlowBuilder);
		final String owner = "spongecell";
		final String group = "supergroup";
		final String dataDir = "/data";
		final String file = dataDir + "/testfile.txt";
		StringEntity createEntity = new StringEntity("Greetings earthling!\n");
		LinkedList<Object> args = new LinkedList<Object>();
		args.add(0, dataDir);
		args.add(1, createEntity);
		args.add(2, file);
		args.add(3, owner);
		args.add(4, group);
		args.add(5, dataDir);
		args.add(6, owner);
		args.add(7, group);
		
		WebHdfsOpsArgs mkdirOpArgs = new WebHdfsOpsArgs(WebHdfsOps.MKDIRS, args.subList(0,1).toArray());
		WebHdfsOpsArgs createOpArgs = new WebHdfsOpsArgs(WebHdfsOps.CREATE, args.subList(1,2).toArray());
		WebHdfsOpsArgs setFileOwnerOpArgs = new WebHdfsOpsArgs(WebHdfsOps.SETOWNER, args.subList(2, 5).toArray());
		WebHdfsOpsArgs setDirOwnerOpArgs = new WebHdfsOpsArgs(WebHdfsOps.SETOWNER, args.subList(5, args.size()).toArray());
		
		WebHdfsWorkFlow workFlow = webHdfsWorkFlowBuilder
			.addEntry("CreateBaseDir", mkdirOpArgs)
			.addEntry("SetBaseDirOwner", setDirOwnerOpArgs)
			.addEntry("CreateAndWriteToFile", createOpArgs)
			.addEntry("SetFileOwner", setFileOwnerOpArgs)
			.build();
		CloseableHttpResponse response = workFlow.execute(); 
		Assert.assertNotNull(response);
		Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.OK.value());
	}
}
