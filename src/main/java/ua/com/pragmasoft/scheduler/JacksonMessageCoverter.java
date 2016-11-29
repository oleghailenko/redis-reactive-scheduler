package ua.com.pragmasoft.scheduler;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Converter;

public class JacksonMessageCoverter extends Converter<Message<?>, String> {

	private ObjectMapper objectMapper;

	public JacksonMessageCoverter() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY);
		this.objectMapper = mapper;
	}

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
