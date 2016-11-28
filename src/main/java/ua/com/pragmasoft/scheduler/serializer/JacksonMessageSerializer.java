package ua.com.pragmasoft.scheduler.serializer;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import ua.com.pragmasoft.scheduler.Message;

/**
 * {@link ObjectMapper} based implementation of {@link MessageSerializer}
 * Please note that defaultTyping of {@link ObjectMapper} should be enabled.
 */
public class JacksonMessageSerializer implements MessageSerializer {

	private ObjectMapper mapper;

	public JacksonMessageSerializer() {
		mapper = new ObjectMapper();
		mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY);
	}

	public JacksonMessageSerializer(ObjectMapper mapper) {
		this.mapper = mapper;
	}

	@Override
	public String serialize(Message message) throws SerializationException {
		try {
			return mapper.writeValueAsString(message);
		} catch (JsonProcessingException e) {
			throw new SerializationException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Message<?> deserilize(String serialized) throws SerializationException {
		try {
			return mapper.readValue(serialized, Message.class);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

}
