package webhdfs.dataloader.scheduler;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.util.Assert;

import spongecell.spring.event_handler.EventHandler;
import spongecell.spring.event_handler.consumer.EventHandlerGenericConsumerTest;
import webhdfs.dataloader.consumer.EventHandlerConsumer;

@Slf4j
@Getter
@EnableConfigurationProperties({ EventHandler.class,
		EventHandlerJobSchedulerConfiguration.class, EventHandlerConsumer.class })
public class EventHandlerJobScheduler {
	@Autowired
	EventHandlerJobSchedulerConfiguration eventHandlerJobSchedulerConfig;
	@Autowired
	private EventHandler<String, String> eventHandlerConsumer;
	@Autowired
	final EventHandlerConsumer<String, String> eventConsumer = new EventHandlerConsumer<String, String>();
	private final ScheduledExecutorService pool = Executors
			.newScheduledThreadPool(1);

	/**
	 * Start the data load.
	 * 
	 * @return
	 * @throws TimeoutException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public EventHandlerConsumer<String, String> loadData()
			throws TimeoutException, InterruptedException, ExecutionException {
		eventHandlerConsumer.groupId("testGroup")
				.topic("audience-server-bluekai").partition(0)
				.keyTranslatorType(String.class)
				.valueTranslatorType(String.class).build();
		Integer initialDelay = eventHandlerJobSchedulerConfig.getInitialDelay();
		Integer period = eventHandlerJobSchedulerConfig.getPeriod();
		TimeUnit timeUnit = eventHandlerJobSchedulerConfig.getTimeUnit();

		ScheduledFuture<?> future = pool.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				final long endTime;
				final long startTime = System.currentTimeMillis();
				eventHandlerConsumer.readAll(eventHandlerConsumer.getTopic(),
						eventConsumer);
				endTime = System.currentTimeMillis();
				log.info("------------------  Load Completed  in {} ", 
					endTime - startTime, "  ------------------------");
			}
		}, initialDelay, period, timeUnit);

		return eventConsumer;
	}

	/**
	 * Shutdown the executor.
	 * 
	 * @throws InterruptedException
	 */
	public void shutdown() throws InterruptedException {
		pool.shutdown();
		pool.awaitTermination(5000, TimeUnit.MILLISECONDS);
		if (!pool.isShutdown()) {
			log.info("Pool is not shutdown.");
			pool.shutdownNow();
		}
		Assert.isTrue(pool.isShutdown());
		log.info("Pool shutdown status : {}", pool.isShutdown());
	}
}
