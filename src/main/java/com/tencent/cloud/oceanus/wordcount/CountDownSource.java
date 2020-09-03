package com.tencent.cloud.oceanus.wordcount;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class CountDownSource implements SourceFunction<String> {
	private long countDown;

	public CountDownSource(long countDownSeconds) {
		if (countDownSeconds > 0) {
			this.countDown = countDownSeconds;
		} else {
			this.countDown = Long.MAX_VALUE;
		}
	}

	@SuppressWarnings("BusyWait")
	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		while (countDown > 0) {
			ctx.collect("This is a test, a really simple test. Can you write a better one? Try it please.");
			Thread.sleep(1000L);
			countDown--;
		}
	}

	@Override
	public void cancel() {
	}
}
