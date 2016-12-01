package ua.com.pragmasoft.scheduler;

import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.hamcrest.CoreMatchers;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.awaitility.Awaitility;

import redis.clients.jedis.Jedis;

/**
 * Test uses Redis on localhost:6379
 */
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
	}

	@Test
	public void scheduleTest() {
		SchedulerTocken schedulerTocken = scheduler.scheduleMessage(Duration.standardSeconds(2), new SomeMessage(1));
		assertThat(schedulerTocken, CoreMatchers.notNullValue());
		assertThat(scheduler.hasMessage(schedulerTocken), CoreMatchers.is(true));
		assertThat(scheduler.getMessage(schedulerTocken).getPayload(), CoreMatchers.is(new SomeMessage(1)));
	}

	@Test
	public void cancelTest() {
		SchedulerTocken schedulerTocken = scheduler.scheduleMessage(Duration.standardSeconds(1), new SomeMessage(1));
		scheduler.cancelMessage(schedulerTocken);
		assertThat(scheduler.hasMessage(schedulerTocken), CoreMatchers.is(false));
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
		SequentialConsumer consumer = new SequentialConsumer();
		scheduler.messageStream().subscribe(consumer);
		Runnable runnable = () -> {
			Scheduler scheduler = new Scheduler(new Jedis("localhost", 6379));
			scheduler.messageStream().subscribe(consumer);
			scheduler.start();
		};
		for(int i = 0; i < 10; i++) {
			new Thread(runnable).start();
		}
		for (int i = 0; i<100; i++) {
			scheduler.scheduleMessage(Duration.millis(1), new SomeMessage(i));
			Thread.sleep(200);
		}
		Awaitility.await().timeout(2, TimeUnit.SECONDS).until(() -> consumer.ints.size() == 100);
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
		Awaitility.await().timeout(8, TimeUnit.SECONDS).until(() -> integer.get() == 2);
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
				log.info(message.getS() + " exist");
				System.exit(5);
			}
			if((System.currentTimeMillis() - someMessageMessage.getTriggerTimestamp()) > TimeUnit.SECONDS.toMillis(2)) {
				log.info("Too late");
				System.exit(6);
			}
			scheduler.cancelMessage(someMessageMessage.getTocken());
			ints.add(message.getS());
			log.info(message.toString());
		}
	}

}