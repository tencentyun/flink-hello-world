package com.tencent.cloud.oceanus.wordcount;

import org.apache.flink.api.java.functions.KeySelector;

public class WordKeySelector implements KeySelector<Counter, String> {

    @Override
    public String getKey(Counter wordWithCount) throws Exception {
        return wordWithCount.getWord();
    }
}
