package ua.com.pragmasoft.scheduler;

import java.util.concurrent.TimeUnit;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;

public class LuaScriptingTest {


	private static String getJobLuaScript = "local currentTimeMS = KEYS[1]\n" +
		"local triggerQueue = KEYS[2]\n" +
		"local workingQueue = KEYS[3]\n" +
		"local jobSet = KEYS[4]\n" +
		"local second = 1000\n" +
		"\n" +
		"local function foundJob(jobID)\n" +
		"\tredis.call('ZREM', triggerQueue, jobID)\n" +
		"\tredis.call('ZADD', workingQueue, currentTimeMS, jobID)\n" +
		"\treturn redis.call('HGET', jobSet, jobID)\n" +
		"end\n" +
		"\n" +
		"local function findNextJob()\n" +
		"\tlocal result\n" +
		"\tresult = redis.call( 'ZRANGEBYSCORE', workingQueue, 0, currentTimeMS-60*second, 'LIMIT',0,1 )\n" +
		"\tif result[1] ~= nil then return foundJob(result[1]) end\n" +
		"\tresult = redis.call( 'ZRANGEBYSCORE', triggerQueue, 0, currentTimeMS, 'LIMIT',0,1 )\n" +
		"\tif result[1] ~= nil then return foundJob(result[1]) end\n" +
		"\treturn nil\n" +
		"end\n" +
		"\n" +
		"return findNextJob()";

	Jedis jedis = new Jedis();
	JacksonMessageCoverter messageCoverter = new JacksonMessageCoverter();

	@Before
	public void setUp() {
		jedis.eval("redis.call('FLUSHALL');");
	}

	@Test
	public void test() {
		String triggerQueue = "scheduler:default:triger";
		String workingQueue = "scheduler:default:working";
		String messageSet = "scheduler:defaut:message";
		long currentTime = System.currentTimeMillis();
		String jobId = "job1";
		String message = messageCoverter.convert(new Message<>(new SomeMessage(5), System.currentTimeMillis(), currentTime, new SchedulerTocken(jobId)));
		jedis.zadd(triggerQueue, currentTime, jobId);
		jedis.hset(messageSet, jobId, message);
		Object result = jedis.eval(getJobLuaScript, 4, String.valueOf(currentTime), triggerQueue, workingQueue, messageSet);
		Assert.assertThat(result, CoreMatchers.is(message));
		currentTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(50);
		result = jedis.eval(getJobLuaScript, 4, String.valueOf(currentTime), triggerQueue, workingQueue, messageSet);
		Assert.assertThat(result, CoreMatchers.nullValue());
		currentTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(65);
		result = jedis.eval(getJobLuaScript, 4, String.valueOf(currentTime), triggerQueue, workingQueue, messageSet);
		Assert.assertThat(result, CoreMatchers.is(message));
	}

}
