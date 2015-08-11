package webhdfs.dataloader.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@SpringBootApplication
@EnableAutoConfiguration
@EnableConfigurationProperties(WebHdfsDataLoaderConfiguration.class)
@EnableWebMvc
public class WebHdfsDataLoaderApplication {
    public static void main(String[] args) {
        SpringApplication.run(WebHdfsDataLoaderApplication.class, args);
    }	
}
