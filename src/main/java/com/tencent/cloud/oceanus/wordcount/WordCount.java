package com.tencent.cloud.oceanus.wordcount;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCount {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 关闭 Operator Chaining, 令运行图更容易初学者理解
		env.disableOperatorChaining();

		// 支持传入参数作为倒计时 (秒)
		int countDownSeconds = -1;
		if (args.length == 1) {
			countDownSeconds = Integer.parseInt(args[0]);
		}

		// 定义数据源
		DataStream<String> streamSource = env
				.addSource(new CountDownSource(countDownSeconds))
				.name("Count down for " + (countDownSeconds <= 0 ? "infinite" : countDownSeconds) + " seconds");

		// 数据处理算子
		DataStream<Counter> dataStream = streamSource
				.flatMap(new WordSplitter())
				.keyBy(new WordKeySelector())
				.reduce(new WordCountReducer()
				);

		// 定义数据目的
		dataStream
				.addSink(new LoggingSink())
				.name("Logging Sink");

		env.execute(WordCount.class.getSimpleName());
	}
}


