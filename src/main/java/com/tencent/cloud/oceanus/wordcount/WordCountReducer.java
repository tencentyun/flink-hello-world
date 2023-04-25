package com.tencent.cloud.oceanus.wordcount;

import org.apache.flink.api.common.functions.ReduceFunction;

public class WordCountReducer implements ReduceFunction<Counter> {

    public Counter reduce(Counter value1, Counter value2) throws Exception {
        return new Counter(value1.getWord(), value1.getCount() + value2.getCount());
    }
}
