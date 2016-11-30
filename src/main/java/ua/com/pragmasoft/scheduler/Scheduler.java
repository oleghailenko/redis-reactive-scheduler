package ua.com.pragmasoft.scheduler;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

/**
 * Simple destributive Scheduler implemenration based on Redis.
 * Instead of executing jobs, this implementation fired {@link Message} into {@link Flux}
 * For example
 * <pre>
 * {@code
 *     Scheduler scheduler = new Scheduler(new JedisPool());
 *     scheduler.start();
 *     scheduler.messageStream().subscribe(Systen.out::println);
 *     scheduler.scheduleMessage(i, TimeUnit.MILLISECONDS, new SomeMessage());
 *     //..and than
 *     scheduler.stop();
 * }
 * </pre>
 */
@Slf4j
public class Scheduler {

	static final String TRIGGERS_QUEUE_NAME = "scheduler:%s:triggers";
	static final String MESSAGE_KEY_NAME = "scheduler:%s:message";

	private final JedisPool jedisPool;
	private final String triggerQueueName;
	private final String messageKeyName;

	private Converter<Message<?>, String> converter;

	private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
	private final Flux<Message<?>> flux;
	private EmitterProcessor<Message<?>> emitterProcessor = EmitterProcessor.<Message<?>>create().connect();

	private Jedis schrdulerConnection;
	private Jedis triggerConnection;
	private Trigger trigger;

	private volatile boolean isRunning = false;

	/**
	 * Build scheduler.
	 * Use {@link JacksonMessageCoverter} as converter.
	 *
	 * @param jedisPool Jedis connection pool
	 */
	public Scheduler(JedisPool jedisPool) {
		this(jedisPool, new JacksonMessageCoverter(), "default");
	}

	/**
	 * Build scheduler with provided parameters
	 *
	 * @param jedisPool        Jedis connection pool
	 * @param converter        Implementation of {@link Converter} from {@link Message<>} to {@link String} and vice versa
	 * @param namespace        Namespace of keys in Redis
	 */
	public Scheduler(JedisPool jedisPool, Converter<Message<?>, String> converter, String namespace) {
		this.jedisPool = jedisPool;
		this.converter = converter;
		this.triggerQueueName = String.format(TRIGGERS_QUEUE_NAME, namespace);
		this.messageKeyName = String.format(MESSAGE_KEY_NAME, namespace);
		flux = Flux.from(emitterProcessor);
	}

	/**
	 * Starts scheduer
	 */
	public void start() {
		Preconditions.checkState(!isRunning, "Scheduler already running.");
		log.info("Starting scheduler");
		schrdulerConnection = jedisPool.getResource();
		triggerConnection = jedisPool.getResource();
		trigger = new Trigger(triggerConnection, emitterProcessor, converter, triggerQueueName, messageKeyName);
		scheduleTrigger(trigger);
		isRunning = true;

	}

	/**
	 * Stops scheduler
	 */
	public void stop() {
		Preconditions.checkState(isRunning, "Scheduler are not running.");
		log.info("Stopping scheduler");
		schrdulerConnection.close();
		schrdulerConnection = null;
		triggerConnection.close();
		triggerConnection = null;
		executorService.shutdown();
		isRunning = false;
	}

	/**
	 * Schedules message with payload in particular time
	 *
	 * @param dateTime Time, when message should be thrown
	 * @param payload   Payload
	 * @param <T>       Class of payload
	 * @return Trigger unique identifier. Can be used for cancel trigger. See {@link this.cancelMessage}
	 * @throws IllegalStateException if schrduler are not running
	 */
	public <T> SchedulerTocken scheduleMessage(DateTime dateTime, T payload) {
		return scheduleMessage(dateTime.getMillis(), payload);
	}

	/**
	 * Schedules message with payload in particular time
	 *
	 * @param duration    Delay from now, when message should be thrown
	 * @param payload  Payload
	 * @param <T>      Class of payload
	 * @return Trigger unique identifier. Can be used for cancel trigger. See {@link this.cancelMessage}
	 * @throws IllegalStateException if schrduler are not running
	 */
	public <T> SchedulerTocken scheduleMessage(Duration duration, T payload) {
		return scheduleMessage(new DateTime().plus(duration.getMillis()), payload);
	}

	/**
	 * Schedules message with payload in particular time
	 *
	 * @param timestamp Time, when message should be thrown
	 * @param payload   Payload
	 * @param <T>       Class of payload
	 * @return Trigger unique identifier. Can be used for cancel trigger. See {@link this.cancelMessage}
	 * @throws IllegalStateException if schrduler are not running
	 */
	private <T> SchedulerTocken scheduleMessage(long timestamp, T payload) {
		Preconditions.checkArgument(payload != null, "Payload can't be null");
		Preconditions.checkState(isRunning, "Scheduler are not running.");
		log.info("Schedule message at {}", new Date(timestamp));
		String id = UUID.randomUUID().toString();
		schrdulerConnection.zadd(triggerQueueName, timestamp, id);
		schrdulerConnection.hset(messageKeyName, id, converter.convert(new Message<>(payload, System.currentTimeMillis(), timestamp)));
		return new SchedulerTocken(id);
	}

	/**
	 * Cancel the message.
	 * @param schedulerTocken Unique identifier. Return value of this.scheduleMessage methods.
	 * @throws IllegalStateException if schrduler are not running
	 */
	public void cancelMessage(SchedulerTocken schedulerTocken) {
		Preconditions.checkState(isRunning, "Scheduler are not running.");
		log.info("Cancel message {}", schedulerTocken);
		schrdulerConnection.hdel(messageKeyName, schedulerTocken.getTocken());
		schrdulerConnection.zrem(triggerQueueName, schedulerTocken.getTocken());
	}

	/**
	 * Check if message with {@link SchedulerTocken} scheduled.
	 * @param schedulerTocken {@link SchedulerTocken} to check
	 * @return true if exist, false otherwise
	 * @throws IllegalStateException if schrduler are not running
	 */
	public boolean hasMessage(SchedulerTocken schedulerTocken) {
		Preconditions.checkState(isRunning, "Scheduler are not running.");
		log.info("Cheking is message {} exist", schedulerTocken);
		return schrdulerConnection.hexists(messageKeyName, schedulerTocken.getTocken());
	}

	/**
	 * Returns message by {@link SchedulerTocken}
	 * @param schedulerTocken {@link SchedulerTocken}
	 * @return Message<>, or null is message with {@link SchedulerTocken} does not exist
	 * @throws IllegalStateException if schrduler are not running
	 */
	public Message<?> getMessage(SchedulerTocken schedulerTocken) {
		Preconditions.checkState(isRunning, "Scheduler are not running.");
		log.info("Getting message {}", schedulerTocken);
		if(hasMessage(schedulerTocken)) {
			return converter.reverse().convert(schrdulerConnection.hget(messageKeyName, schedulerTocken.getTocken()));
		}
		return null;
	}

	/**
	 * Returns {@link Flux} You can listen.
	 * @return flux
	 */
	public Flux<Message<?>> messageStream() {
		return flux;
	}

	private void scheduleTrigger(Trigger trigger) {
		this.executorService.schedule(trigger, 1, TimeUnit.SECONDS);
	}

	/**
	 * Simple Redis Sorted Set listener
	 */
	private class Trigger implements Runnable {

		Logger log = LoggerFactory.getLogger(this.getClass());

		private final Jedis jedis;
		private final EmitterProcessor<Message<?>> emitterProcessor;
		private final Converter<Message<?>, String> converter;
		private final String triggerQueueName;
		private final String messageKeyName;

		public Trigger(Jedis jedis, EmitterProcessor<Message<?>> emitterProcessor) {
			this(jedis, emitterProcessor, new JacksonMessageCoverter(), TRIGGERS_QUEUE_NAME, MESSAGE_KEY_NAME);
		}

		public Trigger(Jedis jedis, EmitterProcessor<Message<?>> emitterProcessor, Converter<Message<?>, String > converter, String triggerQueueName, String messageKeyName) {
			this.jedis = jedis;
			this.emitterProcessor = emitterProcessor;
			this.converter = converter;
			this.triggerQueueName = triggerQueueName;
			this.messageKeyName = messageKeyName;
		}

		@Override
		public void run() {
			log.info("Get triggers...");
			Set<Tuple> triggers;
			do {
				triggers = jedis.zrangeByScoreWithScores(triggerQueueName, 0, System.currentTimeMillis());
				log.info("Got {} triggers", triggers.size());
				if (triggers.size() > 0) {
					jedis.watch(triggerQueueName);
					Transaction transaction = jedis.multi();
					String firstKey = triggers.iterator().next().getElement();
					transaction.zrem(triggerQueueName, firstKey);
					log.info("Delete trigger...");
					List<Object> result = transaction.exec();
					if (!result.isEmpty() && result.get(0).equals(1L)) {
						log.info("We are first");
						publishMessage(firstKey);
					} else {
						log.info("We aren't first");
					}
				}
			} while (triggers.size() > 1);
			Scheduler.this.scheduleTrigger(this);
		}

		@SuppressWarnings("ConstantConditions")
		private void publishMessage(String messageKey) {
			if(jedis.hexists(messageKeyName, messageKey)) {
				Message<?> message = converter.reverse().convert(jedis.hget(messageKeyName, messageKey));
				log.info("Publish message {} {}", messageKey, message);
				jedis.hdel(messageKeyName, messageKey);
				emitterProcessor.onNext(message);
			}
		}
	}
}
