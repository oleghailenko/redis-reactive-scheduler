package ua.com.pragmasoft.scheduler;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class SchedulerJacksonModule extends SimpleModule {

	public SchedulerJacksonModule() {
		super("Scheduler Module");
	}

	@Override
	public void setupModule(SetupContext context) {
		context.setMixInAnnotations(SchedulerTocken.class, TockenMixin.class);
		context.setMixInAnnotations(Message.class, TypedPayload.class);
	}

	@JsonAutoDetect(
		setterVisibility = JsonAutoDetect.Visibility.NONE,
		getterVisibility = JsonAutoDetect.Visibility.NONE,
		creatorVisibility = JsonAutoDetect.Visibility.NONE,
		fieldVisibility = JsonAutoDetect.Visibility.ANY
	)
	private abstract static class TockenMixin {

		@JsonCreator
		public TockenMixin(String tocken) {

		}

		@JsonValue
		public abstract String getTocken();
	}

	private abstract static class TypedPayload<T> {

		@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY)
		public abstract T getPayload();

	}

}
