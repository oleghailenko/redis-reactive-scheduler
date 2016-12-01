package ua.com.pragmasoft.scheduler;

import java.util.Date;
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
import redis.clients.jedis.Transaction;

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

	private final Object mutex = new Object();

	private final Jedis jedis;
	private final String triggerQueueName;
	private final String messageKeyName;

	private Converter<Message<?>, String> converter;

	private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
	private final Flux<Message<?>> flux;
	private EmitterProcessor<Message<?>> emitterProcessor = EmitterProcessor.<Message<?>>create().connect();

	private Trigger trigger;

	private volatile boolean isRunning = false;

	/**
	 * Build scheduler.
	 * Use {@link JacksonMessageCoverter} as converter.
	 *
	 * @param jedis Jedis connection
	 */
	public Scheduler(Jedis jedis) {
		this(jedis, new JacksonMessageCoverter(), "default");
	}

	/**
	 * Build scheduler with provided parameters
	 *
	 * @param jedis     Jedis connection
	 * @param converter Implementation of {@link Converter} from {@link Message<>} to {@link String} and vice versa
	 * @param namespace Namespace of keys in Redis
	 */
	public Scheduler(Jedis jedis, Converter<Message<?>, String> converter, String namespace) {
		this.jedis = jedis;
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
		trigger = new Trigger();
		scheduleTrigger(trigger);
		isRunning = true;

	}

	/**
	 * Stops scheduler
	 */
	public void stop() {
		Preconditions.checkState(isRunning, "Scheduler are not running.");
		log.info("Stopping scheduler");
		jedis.close();
		executorService.shutdown();
		isRunning = false;
	}

	public boolean isRunning() {
		return isRunning || !executorService.isTerminated();
	}

	/**
	 * Schedules message with payload in particular time
	 *
	 * @param dateTime Time, when message should be thrown
	 * @param payload  Payload
	 * @param <T>      Class of payload
	 * @return Trigger unique identifier. Can be used for cancel trigger. See {@link this.cancelMessage}
	 * @throws IllegalStateException if schrduler are not running
	 */
	public <T> SchedulerTocken scheduleMessage(DateTime dateTime, T payload) {
		return scheduleMessage(dateTime.getMillis(), payload);
	}

	/**
	 * Schedules message with payload in particular time
	 *
	 * @param duration Delay from now, when message should be thrown
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
		String id = UUID.randomUUID().toString();
		SchedulerTocken tocken = new SchedulerTocken(id);
		Message<?> message = new Message<>(payload, System.currentTimeMillis(), timestamp, tocken);
		synchronized (mutex) {
			Transaction transaction = jedis.multi();
			transaction.zadd(triggerQueueName, timestamp, id);
			transaction.hset(messageKeyName, id, converter.convert(message));
			transaction.exec();
		}
		if (log.isTraceEnabled()) {
			log.trace("Schedule message {} at {}", message, new Date(timestamp));
		}
		return tocken;
	}

	/**
	 * Cancel the message.
	 *
	 * @param schedulerTocken Unique identifier. Return value of this.scheduleMessage methods.
	 * @throws IllegalStateException if schrduler are not running
	 */
	public void cancelMessage(SchedulerTocken schedulerTocken) {
		Preconditions.checkState(isRunning, "Scheduler are not running.");
		if (log.isTraceEnabled()) {
			log.trace("Cancel message {}", schedulerTocken);
		}
		synchronized (mutex) {
			Transaction transaction = jedis.multi();
			transaction.hdel(messageKeyName, schedulerTocken.getTocken());
			transaction.zrem(triggerQueueName, schedulerTocken.getTocken());
			transaction.exec();
		}
	}

	/**
	 * Check if message with {@link SchedulerTocken} scheduled.
	 *
	 * @param schedulerTocken {@link SchedulerTocken} to check
	 * @return true if exist, false otherwise
	 * @throws IllegalStateException if schrduler are not running
	 */
	public boolean hasMessage(SchedulerTocken schedulerTocken) {
		Preconditions.checkState(isRunning, "Scheduler are not running.");
		if (log.isTraceEnabled()) {
			log.trace("Cheking is message {} exist", schedulerTocken);
		}
		synchronized (mutex) {
			if (jedis.hexists(messageKeyName, schedulerTocken.getTocken())) {
				if (log.isTraceEnabled()) {
					log.trace("Message {} exist", schedulerTocken);
				}
				return true;
			} else {
				if (log.isTraceEnabled()) {
					log.trace("Message {} does not exist", schedulerTocken);
				}
				return false;
			}
		}
	}

	/**
	 * Returns message by {@link SchedulerTocken}
	 *
	 * @param schedulerTocken {@link SchedulerTocken}
	 * @return Message<>, or null is message with {@link SchedulerTocken} does not exist
	 * @throws IllegalStateException if schrduler are not running
	 */
	public Message<?> getMessage(SchedulerTocken schedulerTocken) {
		Preconditions.checkState(isRunning, "Scheduler are not running.");
		if (log.isTraceEnabled()) {
			log.trace("Getting message {}", schedulerTocken);
		}
		if (hasMessage(schedulerTocken)) {
			String serializedMessage;
			synchronized (mutex) {
				serializedMessage = jedis.hget(messageKeyName, schedulerTocken.getTocken());
			}
			return converter.reverse().convert(serializedMessage);
		}
		return null;
	}

	/**
	 * Returns {@link Flux} You can listen.
	 *
	 * @return flux
	 */
	public Flux<Message<?>> messageStream() {
		return flux;
	}

	private void scheduleTrigger(Trigger trigger) {
		if (!this.executorService.isShutdown()) {
			this.executorService.schedule(trigger, 1, TimeUnit.SECONDS);
		}
	}

	/**
	 * Simple Redis Sorted Set listener
	 */
	private class Trigger implements Runnable {

		Logger log = LoggerFactory.getLogger(this.getClass());

		@Override
		public void run() {
			if (log.isTraceEnabled()) {
				log.trace("Get triggers...");
			}
			Message<?> nearestMessage;
			synchronized (mutex) {
				do {
					nearestMessage = getNearestMessage(System.currentTimeMillis());
					if (nearestMessage == null) {
						break;
					} else {
						publishMessage(nearestMessage);
					}
				} while (true);
			}
			Scheduler.this.scheduleTrigger(this);
		}

		private Message<?> getNearestMessage(long currentTimestamp) {
			Object nearestSerializesMessage = jedis.eval(luaScript, 3, String.valueOf(currentTimestamp), triggerQueueName, messageKeyName);
			if (nearestSerializesMessage == null) {
				return null;
			} else {
				String serializedMessage = (String) nearestSerializesMessage;
				return converter.reverse().convert(serializedMessage);
			}
		}

		@SuppressWarnings("ConstantConditions")
		private void publishMessage(Message<?> message) {
			if (log.isTraceEnabled()) {
				log.trace("Publish message {}", message);
			}
			emitterProcessor.onNext(message);
		}

		private String luaScript = "local currentTimeMS = KEYS[1]\n" +
			"local triggerQueue = KEYS[2]\n" +
			"local jobSet = KEYS[3]\n" +
			"local second = 1000\n" +
			"local function foundJob(jobID)\n" +
			"\tredis.call('ZADD', triggerQueue, currentTimeMS + 5*second, jobID)\n" +
			"\treturn redis.call('HGET', jobSet, jobID)\n" +
			"end\n" +
			"local function findNextJob()\n" +
			"\tlocal result\n" +
			"\tresult = redis.call( 'ZRANGEBYSCORE', triggerQueue, 0, currentTimeMS, 'LIMIT',0,1 )\n" +
			"\tif result[1] ~= nil then return foundJob(result[1]) end\n" +
			"\treturn nil\n" +
			"end\n" +
			"return findNextJob()";

	}
}
