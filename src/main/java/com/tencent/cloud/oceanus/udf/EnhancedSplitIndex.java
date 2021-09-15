package com.tencent.cloud.oceanus.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/**
 * 增强版的 ENHANCED_SPLIT_INDEX, 对连续的分隔符会如实处理, 例如输入 "1,2,,3" 会从 ["1","2","","3"] 中取字段
 * Flink 自带的 SPLIT_INDEX 函数会把多个分隔符当作一个, 例如输入 "1,2,,3" 会从 ["1","2","3"] 中取字段
 *
 * SQL 代码声明方式:
 * CREATE TEMPORARY SYSTEM FUNCTION ENHANCED_SPLIT_INDEX AS 'com.tencent.cloud.oceanus.udf.EnhancedSplitIndex' LANGUAGE JAVA;
 */
public class EnhancedSplitIndex extends ScalarFunction {
	private static final Logger LOGGER = LoggerFactory.getLogger(EnhancedSplitIndex.class);

	public String eval(String input, String separator, int index) {
		String[] splits = input.split(Pattern.quote(separator));

		if (index >= splits.length || index < 0) {
			return null;
		}
		return splits[index];
	}

	public static void main(String[] args) {
		EnhancedSplitIndex instance = new EnhancedSplitIndex();
		System.out.println(instance.eval("a|b|c|d|e|f|0|||||||1|0|2|4", "|", 0));
	}
}
