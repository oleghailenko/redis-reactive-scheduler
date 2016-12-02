package ua.com.pragmasoft.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hamcrest.CoreMatchers;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.util.concurrent.AtomicDouble;

import junit.PerformanceTest;
import redis.clients.jedis.Jedis;

@Category(PerformanceTest.class)
public class SchedulerPerformanceTest {

	@Before
	public void setUp() {
		Jedis jedis = new Jedis();
		jedis.eval("redis.call('FLUSHALL')");
		jedis.close();
	}

	@Test
	public void test() throws InterruptedException {
		long currentTime = System.currentTimeMillis();
		Duration duration = Duration.standardMinutes(10);
		long maxEventTime = currentTime + duration.getMillis();
		List<MetricsAggregator> aggregators = Collections.synchronizedList(new ArrayList<>());
		List<Scheduler> schedulers = Collections.synchronizedList(new ArrayList<>());
		AtomicInteger totalScheduled = new AtomicInteger(0);
		AtomicInteger totalConsumed = new AtomicInteger(0);
		AtomicDouble atomicDouble = new AtomicDouble(0);
		Runnable single = () -> {
			MetricsAggregator aggregator = new MetricsAggregator();
			Jedis jedis = new Jedis();
			Scheduler scheduler = new Scheduler(jedis);
			scheduler.setMetricsAggregator(aggregator);
			aggregators.add(aggregator);
			schedulers.add(scheduler);
			scheduler.messageStream().subscribe(message -> {
				totalConsumed.incrementAndGet();
				atomicDouble.addAndGet(System.currentTimeMillis() - message.getTriggerTimestamp());
				scheduler.cancelMessage(message.getToken());
			});
			scheduler.start();
			while (!Thread.interrupted()) {
				scheduler.scheduleMessage(getRandomDurationFromNow(maxEventTime), new SomeMessage(5));
				totalScheduled.incrementAndGet();
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		};
		ExecutorService executorService = Executors.newFixedThreadPool(8);
		for (int i = 0; i < 8; i++) {
			executorService.submit(single);
		}
		Thread.sleep(duration.getMillis() + TimeUnit.SECONDS.toMillis(2));
		executorService.shutdownNow();
		schedulers.forEach(Scheduler::stop);
		MetricsAggregator total = new MetricsAggregator();
		aggregators.forEach(total::withOther);
		Assert.assertThat(totalScheduled.get(), CoreMatchers.is(totalConsumed.get()));
		System.out.println("Metrics: " + total);
		System.out.println("Success rate: " + total.getSuccesses() / (double)total.getTries());
		System.out.println("Failture rate: " + total.getFails() / (double)total.getTries());
		System.out.println("Avarage delay: " + atomicDouble.get() / totalScheduled.get());
	}


	private Random random = new Random();

	private Duration getRandomDurationFromNow(long maxTime) {
		int maxDurationInMills = (int) (maxTime - System.currentTimeMillis());
		return Duration.millis(random.nextInt(maxDurationInMills));
	}

}
