package io.github.zhangjig.transfer.benchmark.util;

import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class Sequences {
	
	private static volatile long START = (System.currentTimeMillis() - 1529864152701L) * 1000000000;
	
	private static final AtomicLong COUNTER = new AtomicLong(0);
	
	static {
		new Timer(true).schedule(new TimerTask() {
			@Override
			public void run() {
				START = (System.currentTimeMillis() - 1529864152701L) * 100000000;
			}
		}, 0, 1000 * 20);
	}
	
	public static long next() {
		return START + (COUNTER.incrementAndGet() % 100000000);
	}
	
	public static long randomLong(long radix) {
		return (long) (Math.random() * radix);
	}
	
	public static String randomString() {
		final Pattern regex = Pattern.compile("-");
		String str = regex.matcher(UUID.randomUUID().toString()).replaceAll("");
		return str.toUpperCase();
	}
}
