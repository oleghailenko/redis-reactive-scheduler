## Redis Reactive scheduller.

Simple message emitter based on Redis Sorted Set.

Example of useage:

```java

public class Test {
	public static void main(String[] args) {
		Scheduler scheduler = new Scheduler(new Jedis());
        scheduler.subscribe().subscribe(Systen.out::println);
        scheduler.scheduleMessage(1, TimeUnit.MILLISECONDS, new SomeMessage());
        Thread.sleep(5000); //Scheduler live while application is live
	}
}

```

