package com.tencent.cloud.oceanus.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class WordSplitter implements FlatMapFunction<String, Counter> {

    public void flatMap(String value, Collector<Counter> out) throws Exception {
        String[] words = value.toLowerCase().split("\\W+");
        for (String word : words) {
            Counter wc = new Counter(word, 1);
            out.collect(wc);
        }
    }
}
