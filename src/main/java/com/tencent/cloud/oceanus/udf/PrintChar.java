package com.tencent.cloud.oceanus.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.stream.IntStream;

/**
 * 逐个打印传入字符串的字符信息
 *
 * SQL 代码声明方式:
 * CREATE TEMPORARY SYSTEM FUNCTION PRINT_CHAR AS 'com.tencent.cloud.oceanus.udf.PrintChar' LANGUAGE JAVA;
 */
public class PrintChar extends ScalarFunction {
	private static final Logger LOGGER = LoggerFactory.getLogger(PrintChar.class);

	public String eval(String input) {
		char[] inputCharArray = input.toCharArray();

		IntStream.range(0, inputCharArray.length).forEach(i -> {
			LOGGER.info("{}", String.format("0x%04X ", (int) inputCharArray[i]));
		});

		return input;
	}

	public static void main(String[] args) {
		PrintChar instance = new PrintChar();
		instance.eval("喵");
	}
}
