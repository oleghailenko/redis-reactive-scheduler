package ua.com.pragmasoft.scheduler;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Converter;

/**
 * Message converter based on Jackson {@link ObjectMapper}
 */
public class JacksonMessageCoverter extends Converter<Message<?>, String> {

	private ObjectMapper objectMapper;

	/**
	 * Creates converter with default {@link ObjectMapper}
	 */
	public JacksonMessageCoverter() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.registerModule(new SchedulerJacksonModule());
		this.objectMapper = mapper;
	}

	/**
	 * Creates converter with specyfyed {@link ObjectMapper}.
	 * @param mapper {@link ObjectMapper}. {@link SchedulerJacksonModule} MUST be registred.
	 */
	public JacksonMessageCoverter(ObjectMapper mapper) {
		this.objectMapper = mapper;
	}

	@Override
	protected String doForward(Message<?> message) {
		try {
			return objectMapper.writeValueAsString(message);
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	protected Message<?> doBackward(String s) {
		try {
			return objectMapper.readValue(s, Message.class);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}
}
