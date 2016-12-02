package ua.com.pragmasoft.scheduler;

import lombok.Getter;
import lombok.ToString;

/**
 *  Metrics aggregator.
 *  Holds number of tries, successes and fails
 */
@Getter
@ToString
public class MetricsAggregator {

	private long tries;
	private long successes;
	private long fails;

	public void withTry() {
		tries++;
	}

	public void withSuccess() {
		successes++;
	}

	public void withFail() {
		fails++;
	}

	public void withOther(MetricsAggregator other) {
		this.tries += other.tries;
		this.successes += other.successes;
		this.fails += other.fails;
	}

}
