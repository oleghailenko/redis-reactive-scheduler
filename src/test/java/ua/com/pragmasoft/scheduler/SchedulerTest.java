package ua.com.pragmasoft.scheduler;

import static org.junit.Assert.assertThat;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.hamcrest.CoreMatchers;
import org.joda.time.Duration;
import org.junit.Test;

import lombok.Data;
import lombok.NonNull;
import redis.clients.jedis.Jedis;

/**
 * Test uses Redis on localhost:6379
 */
public class SchedulerTest {

	private Jedis jedis = new Jedis();
	private Scheduler scheduler = new Scheduler(jedis);

	@Test
	public void scheduleTest() {
		SchedulerTocken schedulerTocken = scheduler.scheduleMessage(Duration.standardSeconds(2), new SomeMessage(1));
		assertThat(schedulerTocken, CoreMatchers.notNullValue());
		assertThat(scheduler.hasMessage(schedulerTocken), CoreMatchers.is(true));
	}

	@Test
	public void cancelTest() {
		SchedulerTocken schedulerTocken = scheduler.scheduleMessage(Duration.standardSeconds(1), new SomeMessage(1));
		scheduler.cancelMessage(schedulerTocken);
		assertThat(scheduler.hasMessage(schedulerTocken), CoreMatchers.is(false));
	}

	@Test
	public void test1() throws InterruptedException {
		scheduler.messageStream().subscribe(new SequentialConsumer());
		scheduler.scheduleMessage(Duration.standardSeconds(2), new SomeMessage(1));
		scheduler.scheduleMessage(Duration.standardSeconds(2), new SomeMessage(2));
		scheduler.scheduleMessage(Duration.standardSeconds(2), new SomeMessage(4));
		scheduler.scheduleMessage(Duration.standardSeconds(2), new SomeMessage(3));
		scheduler.scheduleMessage(Duration.standardSeconds(2), new SomeMessage(5));
		Thread.sleep(6000);
	}

	@Test
	public void test2() throws InterruptedException {
		SequentialConsumer consumer = new SequentialConsumer();
		Runnable runnable = () -> {
			Jedis jedis = new Jedis();
			Scheduler scheduler = new Scheduler(jedis);
			scheduler.messageStream().subscribe(consumer);
		};
		for(int i = 0; i < 10; i++) {
			new Thread(runnable).start();
		}
		for (int i = 0; i<100; i++) {
			scheduler.scheduleMessage(Duration.millis(1), new SomeMessage(i));
			Thread.sleep(200);
		}
	}

	@Data
	public static class SomeMessage {
		@NonNull
		private int s;
	}

	public class SequentialConsumer implements Consumer<Message<?>> {

		private Set<Integer> ints = new HashSet<>();

		@Override
		public void accept(Message<?> someMessageMessage) {
			SomeMessage message = (SomeMessage) someMessageMessage.getPayload();
			if(ints.contains(message.getS())) {
				System.out.println(message.getS() + " exist");
				System.exit(5);
			}
			if((System.currentTimeMillis() - someMessageMessage.getTriggerTimestamp()) > TimeUnit.SECONDS.toMillis(1)) {
				System.exit(6);
			}
			ints.add(message.getS());
			System.out.println(message);
		}
	}

}