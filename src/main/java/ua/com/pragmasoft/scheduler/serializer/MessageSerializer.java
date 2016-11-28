package ua.com.pragmasoft.scheduler.serializer;

import ua.com.pragmasoft.scheduler.Message;

/**
 * {@link Message} serializer.
 */
public interface MessageSerializer {

	/**
	 * Serialize {@link Message} to {@link String}
	 * @param message message object
	 * @param <T> pessage payload class
	 * @return string representation
	 * @throws SerializationException thrown if serializer can't serialize message
	 */
	<T> String serialize(Message<T> message) throws SerializationException;

	/**
	 * Deserialize {@link Message} from {@link String} representation
	 * @param serialized Serialized message
	 * @param <T> Class of message payload
	 * @return Deserialized {@link Message}
	 * @throws SerializationException thrown if deserializer can't deserialize the message
	 */
	<T> Message<T> deserilize(String serialized) throws SerializationException;

}
