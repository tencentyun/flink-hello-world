package com.tencent.cloud.oceanus.wordcount;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingSink implements SinkFunction<Counter> {
	private static final Logger LOGGER = LoggerFactory.getLogger(LoggingSink.class);

	@Override
	public void invoke(Counter value, Context context) throws Exception {
		LOGGER.info("{}: {}", value.getWord(), value.getCount());
	}
}
