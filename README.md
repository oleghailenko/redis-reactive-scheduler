## Redis Reactive scheduller.

Simple message emitter based on Redis Sorted Set.

Example of useage:

Add repo in `pom.xml`
```xml
<repositories>
    <repository>
        <id>redis-reactive-scheduler-mvn-repo</id>
        <url>https://raw.github.com/oleghailenko/redis-reactive-scheduler/tree/mvn-repo/</url>
        <snapshots>
            <enabled>true</enabled>
            <updatePolicy>always</updatePolicy>
        </snapshots>
    </repository>
</repositories>
```

Add dependency
```xml
<dependency>
    <groupId>ua.com.pragmasoft.scheduler</groupId>
    <artifactId>redis-scheduler</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

```java
public class Test {
	public static void main(String[] args) {
		Scheduler scheduler = new Scheduler(new Jedis());
        scheduler.messageStream().subscribe(Systen.out::println);
        scheduler.scheduleMessage(1, TimeUnit.MILLISECONDS, new SomeMessage());
        Thread.sleep(5000); //Scheduler live while application is live
	}
}

```

