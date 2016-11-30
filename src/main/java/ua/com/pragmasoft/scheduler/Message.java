package ua.com.pragmasoft.scheduler;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import lombok.*;


/**
 * Represent a message, that will thrown to subscribers in trigger time.
 * Immutable class. Use {@link MessageBuilder} for creation
 *
 * @param <T> Class of payload
 */
@Value
@Builder
@AllArgsConstructor(access = AccessLevel.PUBLIC)
public class Message<T> {

	@NonNull
	private T payload;

	/**
	 * Time, when message should be fired
	 */
	private long scheduledTimestamp;

	/**
	 * Time, when message has been schduled
	 */
	@NonNull
	private long triggerTimestamp;

	/**
	 * Scheduler tocken
	 */
	@NonNull
	private SchedulerTocken tocken;

}
