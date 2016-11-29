package ua.com.pragmasoft.scheduler;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Converter;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.EmitterProcessor;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

/**
 * Simple Redis Sorted Set listener
 */
@Slf4j
public class Trigger implements Runnable {

	private final Jedis jedis;
	private final EmitterProcessor<Message<?>> emitterProcessor;
	private final Converter<Message<?>, String> converter;
	private final String triggerQueueName;
	private final String messageKeyName;

	public Trigger(Jedis jedis, EmitterProcessor<Message<?>> emitterProcessor) {
		this(jedis, emitterProcessor, new JacksonMessageCoverter(), Scheduler.TRIGGERS_QUEUE_NAME, Scheduler.MESSAGE_KEY_NAME);
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
		Set<Tuple> triggers = jedis.zrangeByScoreWithScores(triggerQueueName, 0, System.currentTimeMillis());
		log.info("Got {} triggers", triggers.size());
		if (triggers.size() > 0) {
			jedis.watch(triggerQueueName);
			Transaction transaction = jedis.multi();
			Set<String> triggerKeys = new HashSet<>(triggers.size());
			triggerKeys.addAll(triggers.stream().map(Tuple::getElement).collect(Collectors.toList()));
			transaction.zrem(triggerQueueName, triggerKeys.toArray(new String[0]));
			log.info("Delete triggers...");
			List<Object> result = transaction.exec();
			if (!result.isEmpty() && result.get(0).equals(triggerKeys.size())) {
				log.info("We are first");
				triggerKeys.forEach(this::publishMessage);
			} else {
				log.info("We aren't first");
			}
		}
	}

	@SuppressWarnings("ConstantConditions")
	private void publishMessage(String messageKey) {
		if(jedis.hexists(messageKeyName, messageKey)) {
			Message<?> message = converter.reverse().convert(jedis.hget(messageKeyName, messageKey));
			log.info("Publish message {}", message);
			jedis.hdel(messageKeyName, messageKey);
			emitterProcessor.onNext(message);
		}
	}
}
