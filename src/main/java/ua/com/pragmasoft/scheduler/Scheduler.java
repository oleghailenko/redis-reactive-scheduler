package ua.com.pragmasoft.scheduler;

import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import redis.clients.jedis.Jedis;

/**
 * Simple destributive Scheduler implemenration based on Redis.
 * Instead of executing jobs, this implementation fired {@link Message} into {@link Flux}
 * For example
 * <pre>
 * {@code
 *     Scheduler scheduler = new Scheduler(new Jedis());
 *     scheduler.messageStream().subscribe(Systen.out::println);
 *     scheduler.scheduleMessage(i, TimeUnit.MILLISECONDS, new SomeMessage());
 * }
 * </pre>
 */
@Slf4j
public class Scheduler {

	static final String TRIGGERS_QUEUE_NAME = "scheduler:%s:triggers";
	static final String MESSAGE_KEY_NAME = "scheduler:%s:message";

	private final Jedis jedis;
	private final String triggerQueueName;
	private final String messageKeyName;

	private Converter<Message<?>, String> converter;

	private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
	private final Flux<Message<?>> flux;
	private EmitterProcessor<Message<?>> emitterProcessor = EmitterProcessor.<Message<?>>create().connect();

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
	 * @param jedis            Jedis connection
	 * @param conventer        Implementation of {@link Converter} from {@link Message<>} to {@link String} and vice versa
	 * @param namespace        Namespace of keys in Redis
	 */
	public Scheduler(Jedis jedis, Converter<Message<?>, String> conventer, String namespace) {
		this.jedis = jedis;
		this.converter = conventer;
		this.triggerQueueName = String.format(TRIGGERS_QUEUE_NAME, namespace);
		this.messageKeyName = String.format(MESSAGE_KEY_NAME, namespace);
		flux = Flux.from(emitterProcessor);
		executorService.scheduleWithFixedDelay(new Trigger(jedis, emitterProcessor, conventer, triggerQueueName, messageKeyName), 1, 1, TimeUnit.SECONDS);
	}

	/**
	 * Schedules message with payload in particular time
	 *
	 * @param timestamp Time, when message should be thrown
	 * @param payload   Payload
	 * @param <T>       Class of payload
	 * @return Trigger unique identifier. Can be used for cancel trigger. See {@link this.cancelMessage}
	 */
	public <T> SchedulerTocken scheduleMessage(long timestamp, T payload) {
		return scheduleMessage(timestamp, payload, null);
	}

	/**
	 * Schedules message with payload in particular time
	 *
	 * @param delay    Delay from now, when message should be thrown
	 * @param timeUnit Delay time unit one of {@link TimeUnit}
	 * @param payload  Payload
	 * @param <T>      Class of payload
	 * @return Trigger unique identifier. Can be used for cancel trigger. See {@link this.cancelMessage}
	 */
	public <T> SchedulerTocken scheduleMessage(long delay, TimeUnit timeUnit, T payload) {
		return scheduleMessage(new Date().getTime() + timeUnit.toMillis(delay), payload);
	}

	/**
	 * Schedules message with payload in particular time
	 *
	 * @param delay    Delay from now, when message should be thrown
	 * @param timeUnit Delay time unit one of {@link TimeUnit}
	 * @param payload  Payload
	 * @param <T>      Class of payload
	 * @param headers   Map with addition headers
	 * @return Trigger unique identifier. Can be used for cancel trigger. See {@link this.cancelMessage}
	 */
	public <T> SchedulerTocken scheduleMessage(long delay, TimeUnit timeUnit, T payload, Map<String, Object> headers) {
		return scheduleMessage(new Date().getTime() + timeUnit.toMillis(delay), payload, headers);
	}

	/**
	 * Schedules message with payload in particular time
	 *
	 * @param timestamp Time, when message should be thrown
	 * @param payload   Payload
	 * @param <T>       Class of payload
	 * @param headers    Map with addition headers
	 * @return Trigger unique identifier. Can be used for cancel trigger. See {@link this.cancelMessage}
	 */
	public <T> SchedulerTocken scheduleMessage(long timestamp, T payload, Map<String, Object> headers) {
		Preconditions.checkArgument(payload != null, "Payload can't be null");
		log.info("Schedule message at {}", new Date(timestamp));
		String id = UUID.randomUUID().toString();
		jedis.zadd(triggerQueueName, timestamp, id);
		jedis.hset(messageKeyName, id, converter.convert(new Message<>(payload, System.currentTimeMillis(), timestamp)));
		return new SchedulerTocken(id);
	}

	/**
	 * Cancel the message.
	 * @param schedulerTocken Unique identifier. Return value of this.scheduleMessage methods.
	 */
	public void cancelMessage(SchedulerTocken schedulerTocken) {
		log.info("Cancel message {}", schedulerTocken);
		jedis.hdel(messageKeyName, schedulerTocken.getTocken());
		jedis.zrem(triggerQueueName, schedulerTocken.getTocken());
	}

	public boolean hasMessage(SchedulerTocken schedulerTocken) {
		log.info("Cheking is message {} exist", schedulerTocken);
		return jedis.hexists(messageKeyName, schedulerTocken.getTocken());
	}

	/**
	 * Returns {@link Flux} You can listen.
	 * @return flux
	 */
	public Flux<Message<?>> messageStream() {
		return flux;
	}

}
