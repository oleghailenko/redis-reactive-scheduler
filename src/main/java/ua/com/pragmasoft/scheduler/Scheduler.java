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

	private final Object mutex = new Object();

	private final Jedis jedis;
	private final String triggerQueueName;
	private final String messageKeyName;

	private Converter<Message<?>, String> converter;

	private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
	private final Flux<Message<?>> flux;
	private EmitterProcessor<Message<?>> emitterProcessor = EmitterProcessor.<Message<?>>create().connect();

	private Trigger trigger;
	private MetricsAggregator metricsAggregator;

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
		metricsAggregator = null;
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
	public <T> SchedulerToken scheduleMessage(DateTime dateTime, T payload) {
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
	public <T> SchedulerToken scheduleMessage(Duration duration, T payload) {
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
	private <T> SchedulerToken scheduleMessage(long timestamp, T payload) {
		Preconditions.checkArgument(payload != null, "Payload can't be null");
		Preconditions.checkState(isRunning, "Scheduler are not running.");
		String id = UUID.randomUUID().toString();
		SchedulerToken tocken = new SchedulerToken(id);
		Message<?> message = new Message<>(payload, getCurrentTimeMills(), timestamp, tocken, 1);
		synchronized (mutex) {
			List<Object> result;
			do {
				Transaction transaction = jedis.multi();
				transaction.zadd(triggerQueueName, timestamp, id);
				transaction.hset(messageKeyName, id, converter.convert(message));
				result = transaction.exec();
			} while (result.isEmpty());
		}
		if (log.isTraceEnabled()) {
			log.trace("Schedule message {} at {}", message, new Date(timestamp));
		}
		return tocken;
	}

	/**
	 * Moved firing the message
	 *
	 * @param duration Amount of time.
	 * @param token    Trigger unique identifier.
	 */
	public void rescheduleMessage(Duration duration, SchedulerToken token) {
		Preconditions.checkArgument(duration != null, "duration can't be null");
		Preconditions.checkArgument(token != null, "token can't be null");
		if (log.isTraceEnabled()) {
			log.trace("Reschedule message {} by {}", token, duration);
		}
		Message<?> message = getMessage(token);
		message = message.withScheduledTimestamp(message.getScheduledTimestamp() + duration.getMillis());
		synchronized (mutex) {
			List<Object> result;
			do {
				Transaction transaction = jedis.multi();
				transaction.zincrby(triggerQueueName, duration.getMillis(), token.getToken());
				transaction.hset(messageKeyName, token.getToken(), converter.convert(message));
				result = transaction.exec();
			} while (result.isEmpty());
		}
	}

	/**
	 * Cancel the message.
	 *
	 * @param schedulerToken Unique identifier. Return value of this.scheduleMessage methods.
	 * @throws IllegalStateException if schrduler are not running
	 */
	public void cancelMessage(SchedulerToken schedulerToken) {
		Preconditions.checkState(isRunning, "Scheduler are not running.");
		if (log.isTraceEnabled()) {
			log.trace("Cancel message {}", schedulerToken);
		}
		synchronized (mutex) {
			List<Object> result;
			do {
				Transaction transaction = jedis.multi();
				transaction.hdel(messageKeyName, schedulerToken.getToken());
				transaction.zrem(triggerQueueName, schedulerToken.getToken());
				result = transaction.exec();
			} while (result.isEmpty());

		}
	}

	/**
	 * Check if message with {@link SchedulerToken} scheduled.
	 *
	 * @param schedulerToken {@link SchedulerToken} to check
	 * @return true if exist, false otherwise
	 * @throws IllegalStateException if schrduler are not running
	 */
	public boolean hasMessage(SchedulerToken schedulerToken) {
		Preconditions.checkState(isRunning, "Scheduler are not running.");
		if (log.isTraceEnabled()) {
			log.trace("Cheking is message {} exist", schedulerToken);
		}
		synchronized (mutex) {
			if (jedis.hexists(messageKeyName, schedulerToken.getToken())) {
				if (log.isTraceEnabled()) {
					log.trace("Message {} exist", schedulerToken);
				}
				return true;
			} else {
				if (log.isTraceEnabled()) {
					log.trace("Message {} does not exist", schedulerToken);
				}
				return false;
			}
		}
	}

	/**
	 * Returns message by {@link SchedulerToken}
	 *
	 * @param schedulerToken {@link SchedulerToken}
	 * @return Message<>, or null is message with {@link SchedulerToken} does not exist
	 * @throws IllegalStateException if schrduler are not running
	 */
	public Message<?> getMessage(SchedulerToken schedulerToken) {
		Preconditions.checkState(isRunning, "Scheduler are not running.");
		if (log.isTraceEnabled()) {
			log.trace("Getting message {}", schedulerToken);
		}
		if (hasMessage(schedulerToken)) {
			String serializedMessage;
			synchronized (mutex) {
				serializedMessage = jedis.hget(messageKeyName, schedulerToken.getToken());
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

	/**
	 * Optional {@link MetricsAggregator}
	 *
	 * @param metricsAggregator must be not null
	 */
	public void setMetricsAggregator(MetricsAggregator metricsAggregator) {
		Preconditions.checkArgument(metricsAggregator != null, "metrixAggregator must be not null value");
		this.metricsAggregator = metricsAggregator;
	}

	private void scheduleTrigger(Trigger trigger) {
		if (!this.executorService.isShutdown()) {
			this.executorService.schedule(trigger, 1, TimeUnit.SECONDS);
		}
	}

	private long getCurrentTimeMills() {
		return new DateTime().getMillis();
	}

	private void notifyTry() {
		if (metricsAggregator != null) {
			metricsAggregator.withTry();
		}
	}

	private void notifySuccess() {
		if (metricsAggregator != null) {
			metricsAggregator.withSuccess();
		}
	}

	private void notifyFail() {
		if (metricsAggregator != null) {
			metricsAggregator.withFail();
		}
	}

	private void trace(String format, Object... arguments) {
		if (log.isTraceEnabled()) {
			log.trace(format, arguments);
		}
	}

	/**
	 * Simple Redis Sorted Set listener
	 */
	private class Trigger implements Runnable {

		Logger log = LoggerFactory.getLogger(this.getClass());

		@Override
		public void run() {
			trace("Get triggers...");
			Set<Tuple> triggers;
			synchronized (mutex) {
				try {
					do {
						jedis.watch(triggerQueueName, messageKeyName);
						notifyTry();
						triggers = jedis.zrangeByScoreWithScores(triggerQueueName, 0, getCurrentTimeMills(), 0, 1);
						trace("Got triggers {}", triggers.size());
						if (!triggers.isEmpty()) {
							Tuple firstElem = triggers.iterator().next();
							Message<?> message = getMessage(new SchedulerToken(firstElem.getElement()));
							if (message != null && message.getAttempt() < 5) {
								Transaction transaction = jedis.multi();
								transaction.zincrby(triggerQueueName, TimeUnit.MINUTES.toMillis(5), firstElem.getElement());
								transaction.hset(messageKeyName, firstElem.getElement(), converter.convert(
									message
										.withAttempt(message.getAttempt() + 1)
										.withTriggerTimestamp(getCurrentTimeMills())
									)
								);
								trace("Reschedule trigger...");
								List<Object> result = transaction.exec();
								if (!result.isEmpty()) {
									trace("We are first, {}", firstElem.getElement());
									publishMessage(message);
									notifySuccess();
								} else {
									trace("We aren't first");
									notifyFail();
								}
							} else {
								notifySuccess();
								List<Object> result;
								do {
									Transaction transaction = jedis.multi();
									transaction.zrem(triggerQueueName, firstElem.getElement());
									transaction.hdel(messageKeyName, firstElem.getElement());
									result = transaction.exec();
								} while (result.isEmpty());

							}
						} else {
							notifyFail();
						}
					} while (!triggers.isEmpty());
				} catch (Exception e) {
					log.error("Exceprion has been occurred", e);
				}
			}
			Scheduler.this.scheduleTrigger(this);
		}

		@SuppressWarnings("ConstantConditions")
		private void publishMessage(Message<?> message) {
			if (log.isTraceEnabled()) {
				log.trace("Publish message {}", message);
			}
			emitterProcessor.onNext(message);
		}
	}
}
