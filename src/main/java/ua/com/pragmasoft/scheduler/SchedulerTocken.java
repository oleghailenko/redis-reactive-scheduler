package ua.com.pragmasoft.scheduler;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;

@Value
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class SchedulerTocken {
	@NonNull
	private String tocken;
}
