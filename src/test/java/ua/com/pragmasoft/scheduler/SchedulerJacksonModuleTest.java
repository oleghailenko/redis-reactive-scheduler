package ua.com.pragmasoft.scheduler;

import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SchedulerJacksonModuleTest {

	@Test
	@SuppressWarnings("unchecked")
	public void test() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		mapper.registerModule(new SchedulerJacksonModule());
		Message<SomeMessage> original =new Message<>(new SomeMessage(50), 500, 500, new SchedulerToken("tocken"));
		String serialized = mapper.writeValueAsString(original);
		Message<SomeMessage> deserialized = mapper.readValue(serialized, Message.class);
		assertThat(original, CoreMatchers.is(deserialized));
	}

}