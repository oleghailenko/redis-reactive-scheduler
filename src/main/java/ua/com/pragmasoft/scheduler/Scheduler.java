package ua.com.pragmasoft.scheduler;

import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import redis.clients.jedis.Jedis;
import ua.com.pragmasoft.scheduler.serializer.JacksonMessageSerializer;
import ua.com.pragmasoft.scheduler.serializer.MessageSerializer;

/**
 * Simple destributive Scheduler implemenration based on Redis.
 * Instead of executing jobs, this implementation fired {@link Message} into {@link Flux}
 * For example
 * <pre>
 * {@code
 *     Scheduler scheduler = new Scheduler(new Jedis());
 *     scheduler.subscribe().subscribe(Systen.out::println);
 *     scheduler.scheduleMessage(i, TimeUnit.MILLISECONDS, new SomeMessage());
 * }
 * </pre>
 */
@Slf4j
public class Scheduler {

	static final String TRIGGERS_QUEUE_NAME = "message:triggers";
	static final String MESSAGE_KEY_NAME = "message";

	private final Jedis jedis;
	private final MessageSerializer serializer;
	private final String triggerQueueName;
	private final String messageKeyName;

	private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
	private final Flux<Message<?>> flux;
	private EmitterProcessor<Message<?>> emitterProcessor = EmitterProcessor.<Message<?>>create().connect();

	/**
	 * Build scheduler.
	 * Use {@link JacksonMessageSerializer} as serializer.
	 *
	 * @param jedis Jedis connection
	 */
	public Scheduler(Jedis jedis) {
		this(jedis, new JacksonMessageSerializer(), TRIGGERS_QUEUE_NAME, MESSAGE_KEY_NAME);
	}

	/**
	 * Build scheduler with provided parameters
	 *
	 * @param jedis            Jedis connection
	 * @param serializer       Implementation of {@link MessageSerializer}
	 * @param triggerQueueName name of sorted set for triggers
	 * @param messageKeyName   name of hash set for messages
	 */
	public Scheduler(Jedis jedis, MessageSerializer serializer, String triggerQueueName, String messageKeyName) {
		this.jedis = jedis;
		this.serializer = serializer;
		this.triggerQueueName = triggerQueueName;
		this.messageKeyName = messageKeyName;
		flux = Flux.from(emitterProcessor);
		executorService.scheduleWithFixedDelay(new Trigger(jedis, emitterProcessor, serializer, triggerQueueName, messageKeyName), 1, 1, TimeUnit.SECONDS);
	}

	/**
	 * Schedules message with payload in particular time
	 *
	 * @param timestamp Time, when message should be thrown
	 * @param payload   Payload
	 * @param <T>       Class of payload
	 * @return Trigger unique identifier. Can be used for cancel trigger. See {@link this.cancelMessage}
	 */
	public <T> String scheduleMessage(long timestamp, T payload) {
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
	public <T> String scheduleMessage(long delay, TimeUnit timeUnit, T payload) {
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
	public <T> String scheduleMessage(long delay, TimeUnit timeUnit, T payload, Map<String, Object> headers) {
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
	public <T> String scheduleMessage(long timestamp, T payload, Map<String, Object> headers) {
		Preconditions.checkArgument(payload != null, "Payload can't be null");
		log.info("Schedule message at {}", new Date(timestamp));
		String id = UUID.randomUUID().toString();
		jedis.zadd(triggerQueueName, timestamp, id);
		jedis.hset(messageKeyName, id, serializer.serialize(new Message.MessageBuilder<T>().withPayload(payload).withHeaders(headers).withTriggerTime(new Date(timestamp)).build()));
		return id;
	}

	/**
	 * Cancel the message.
	 * @param messageId Unique identifier. Return value of this.scheduleMessage methods.
	 */
	public void cancelMessage(String messageId) {
		log.info("Cancel message {}", messageId);
		jedis.hdel(messageKeyName, messageId);
		jedis.zrem(triggerQueueName, messageId);
	}

	/**
	 * Returns {@link Flux} You can listen.
	 * @return flux
	 */
	public Flux<Message<?>> subscribe() {
		return flux;
	}

}
