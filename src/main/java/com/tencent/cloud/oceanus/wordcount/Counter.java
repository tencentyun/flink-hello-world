package com.tencent.cloud.oceanus.wordcount;

public class Counter {
    private String word;
    private int count;

    public Counter() {}

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    Counter(String word, int count) {
        this.word = word;
        this.count = count;
    }

    public String toString() {
        return "word: " + this.word + ", count: " + this.count;
    }
}
