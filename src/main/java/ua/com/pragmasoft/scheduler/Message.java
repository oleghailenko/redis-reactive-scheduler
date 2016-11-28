package ua.com.pragmasoft.scheduler;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import lombok.NonNull;
import lombok.Value;


/**
 * Represent a message, that will thrown to subscribers in trigger time.
 * Immutable class. Use {@link MessageBuilder} for creation
 *
 * @param <T> Class of payload
 */
@Value
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class Message<T> {

	private static final String SCHEDULED_TIME_KEY = "scheduled_time";
	private static final String TRIGGER_TIME_KEY = "trigger_time";

	@SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
	@NonNull
	private Map<String, Object> headers;

	@NonNull
	private T payload;

	/**
	 * Time, whan message should be fired
	 *
	 * @return Time
	 */
	public Date getTriggerTime() {
		return (Date) headers.get(TRIGGER_TIME_KEY);
	}

	/**
	 * Time, whan message has been schduled
	 *
	 * @return Time
	 */
	public Date getScheduleTime() {
		return (Date) headers.get(SCHEDULED_TIME_KEY);
	}

	public static class MessageBuilder<T> {

		private T payload;
		private Map<String, Object> headers;

		public MessageBuilder() {
			headers = new HashMap<>();
		}

		/**
		 * Set {@link Message} payload
		 *
		 * @param payload Payload
		 * @return this
		 */
		public MessageBuilder<T> withPayload(T payload) {
			this.payload = payload;
			return this;
		}

		/**
		 * Set time, whan {@link Message} has been scheduled.
		 *
		 * @param date time
		 * @return this
		 */
		public MessageBuilder<T> withScheduledTime(Date date) {
			this.headers.put(SCHEDULED_TIME_KEY, date);
			return this;
		}

		/**
		 * Set time, whan {@link Message} should be thrown.
		 *
		 * @param date time
		 * @return this
		 */
		public MessageBuilder<T> withTriggerTime(Date date) {
			this.headers.put(TRIGGER_TIME_KEY, date);
			return this;
		}

		/**
		 * Set custon header
		 *
		 * @param key   Key of header
		 * @param value Value of header. {@link ua.com.pragmasoft.scheduler.serializer.MessageSerializer} must know how to work with it
		 * @return this
		 */
		public MessageBuilder<T> withHeader(String key, Object value) {
			this.headers.put(key, value);
			return this;
		}

		/**
		 * Copy all entries from nulltable headers
		 * @param headers Map with headers
		 * @return this
		 */
		public MessageBuilder<T> withHeaders(Map<String, Object> headers) {
			if (headers != null) {
				headers.entrySet().forEach(entry -> {
					this.headers.put(entry.getKey(), entry.getValue());
				});
			}
			return this;
		}

		/**
		 * Build the message
		 *
		 * @return Immutable {@link Message} with provided payload and headers.
		 * @throws IllegalStateException if trigger time hasn't been set
		 */
		public Message<T> build() {
			Preconditions.checkState(headers.containsKey(TRIGGER_TIME_KEY), "You must specify trigger time");
			this.headers.putIfAbsent(SCHEDULED_TIME_KEY, new Date());
			return new Message<T>(ImmutableMap.copyOf(headers), payload);
		}

	}

}
