package ua.com.pragmasoft.scheduler;

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
	 * Scheduler token
	 */
	@NonNull
	private SchedulerToken token;

}
