package ua.com.pragmasoft.scheduler;

import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.hamcrest.CoreMatchers;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.awaitility.Awaitility;

import junit.IntegrationTest;
import redis.clients.jedis.Jedis;

/**
 * Test uses Redis on localhost:6379
 */
@Category(IntegrationTest.class)
public class SchedulerTest {

	private Jedis jedis;
	private Scheduler scheduler;

	@Before
	public void setUp() {
		jedis = new Jedis("localhost", 6379);
		scheduler = new Scheduler(jedis);
		jedis.eval("return redis.call('FLUSHALL')");
		scheduler.start();
	}

	@After
	public void shutdown() {
		scheduler.stop();
		jedis.close();
		Awaitility.await().timeout(5, TimeUnit.SECONDS).until(() -> !scheduler.isRunning());
		DateTimeUtils.setCurrentMillisSystem();
	}

	@Test
	public void scheduleTest() {
		SchedulerToken schedulerToken = scheduler.scheduleMessage(Duration.standardSeconds(2), new SomeMessage(1));
		assertThat(schedulerToken, CoreMatchers.notNullValue());
		assertThat(scheduler.hasMessage(schedulerToken), CoreMatchers.is(true));
		assertThat(scheduler.getMessage(schedulerToken).getPayload(), CoreMatchers.is(new SomeMessage(1)));
	}

	@Test
	public void cancelTest() {
		SchedulerToken schedulerToken = scheduler.scheduleMessage(Duration.standardSeconds(1), new SomeMessage(1));
		scheduler.cancelMessage(schedulerToken);
		assertThat(scheduler.hasMessage(schedulerToken), CoreMatchers.is(false));
	}

	@Test
	public void test1() throws InterruptedException {
		SequentialConsumer consumer = new SequentialConsumer();
		scheduler.messageStream().subscribe(consumer);
		scheduler.scheduleMessage(Duration.standardSeconds(2), new SomeMessage(1));
		scheduler.scheduleMessage(Duration.standardSeconds(2), new SomeMessage(2));
		scheduler.scheduleMessage(Duration.standardSeconds(2), new SomeMessage(4));
		scheduler.scheduleMessage(Duration.standardSeconds(2), new SomeMessage(3));
		scheduler.scheduleMessage(Duration.standardSeconds(2), new SomeMessage(5));
		Awaitility.await().timeout(5, TimeUnit.SECONDS).until(() -> consumer.ints.size() == 5);
	}

	@Test
	public void test2() throws InterruptedException {
		int numEvents = 100;
		ArrayList<Scheduler> schedulers = new ArrayList<>();
		try {
			SequentialConsumer consumer = new SequentialConsumer();
			scheduler.messageStream().subscribe(consumer);
			Runnable runnable = () -> {
				Scheduler scheduler = new Scheduler(new Jedis("localhost", 6379));
				scheduler.messageStream().subscribe(consumer);
				scheduler.start();
				schedulers.add(scheduler);
			};
			for(int i = 0; i < 10; i++) {
				new Thread(runnable).start();
			}
			for (int i = 0; i<numEvents; i++) {
				scheduler.scheduleMessage(Duration.millis(1), new SomeMessage(i));
				Thread.sleep(200);
			}
			Awaitility.await().timeout(2, TimeUnit.SECONDS).until(() -> consumer.ints.size() == numEvents);
		} finally {
			schedulers.forEach(Scheduler::stop);
		}

	}

	@Test
	public void acknowledgeTest() {
		AtomicInteger integer = new AtomicInteger(0);
		scheduler.messageStream().subscribe(message -> {
			assertThat(message.getPayload(), CoreMatchers.instanceOf(SomeMessage.class));
			assertThat(((SomeMessage) message.getPayload()).getS(), CoreMatchers.is(1));
			integer.incrementAndGet();
		});
		scheduler.scheduleMessage(Duration.standardSeconds(1), new SomeMessage(1));
		Awaitility.await().timeout(3, TimeUnit.SECONDS).until(() -> integer.get() == 1);
		DateTimeUtils.setCurrentMillisOffset(Duration.standardMinutes(5).getMillis());
		Awaitility.await().timeout(3, TimeUnit.SECONDS).until(() -> integer.get() == 2);
		DateTimeUtils.setCurrentMillisSystem();
	}

	@Test
	public void rescheduleTest() {
		SchedulerToken token = scheduler.scheduleMessage(Duration.standardSeconds(5), new SomeMessage(1));
		long originalTime = scheduler.getMessage(token).getScheduledTimestamp();
		scheduler.rescheduleMessage(Duration.standardDays(1), token);
		assertThat(scheduler.getMessage(token).getScheduledTimestamp() - originalTime, CoreMatchers.is(Duration.standardDays(1).getMillis()));
	}

	@Test
	public void metricsAggregatorTest() throws InterruptedException {
		MetricsAggregator aggregator = new MetricsAggregator();
		CountDownLatch latch = new CountDownLatch(1);
		scheduler.messageStream().subscribe(message -> {
			latch.countDown();
		});
		scheduler.setMetricsAggregator(aggregator);
		scheduler.scheduleMessage(Duration.millis(1), new SomeMessage(1));
		latch.await();
		assertThat(aggregator.getTries(), CoreMatchers.is(1L));
		assertThat(aggregator.getSuccesses(), CoreMatchers.is(1L));
	}

	public class SequentialConsumer implements Consumer<Message<?>> {

		Logger log = LoggerFactory.getLogger(this.getClass());

		private Set<Integer> ints;

		public SequentialConsumer() {
			this.ints = Collections.synchronizedSet(new HashSet<>());
		}

		@Override
		public void accept(Message<?> someMessageMessage) {
			SomeMessage message = (SomeMessage) someMessageMessage.getPayload();
			if(ints.contains(message.getS())) {
				log.error(message.getS() + " exist");
				System.exit(5);
			}
			if((System.currentTimeMillis() - someMessageMessage.getTriggerTimestamp()) > TimeUnit.SECONDS.toMillis(2)) {
				log.error("Too late");
				System.exit(6);
			}
			scheduler.cancelMessage(someMessageMessage.getToken());
			ints.add(message.getS());
			log.info(message.toString());
		}
	}

}