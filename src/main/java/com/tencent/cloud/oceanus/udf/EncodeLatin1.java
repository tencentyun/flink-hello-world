package com.tencent.cloud.oceanus.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.stream.IntStream;

/**
 * 如果 JDBC 数据库的 VARCHAR 为 Latin1 (或 GBK 等) 编码
 * 可以使用这个函数将 Java 字符串编码后写入
 *
 * SQL 代码声明方式:
 * CREATE TEMPORARY SYSTEM FUNCTION ENCODE_LATIN1 AS 'com.tencent.cloud.oceanus.udf.EncodeLatin1' LANGUAGE JAVA;
 */
public class EncodeLatin1 extends ScalarFunction {
	private static final Logger LOGGER = LoggerFactory.getLogger(EncodeLatin1.class);

	public String eval(String input) {
		return eval(input, "latin1");
	}

	public String eval(String input, String toCharset) {
		try {
			byte[] inputBytes = input.getBytes(toCharset);
			char[] chars = new char[inputBytes.length];

			IntStream.range(0, inputBytes.length).forEach(i -> {
				chars[i] = (char) inputBytes[i];
				chars[i] = (char) (0x00FF & chars[i]);
			});
			return new String(chars);
		} catch (UnsupportedEncodingException e) {
			// 与 GET_JSON_OBJECT 的异常处理方式保持一致, 遇到异常数据时输出 null, 避免日志过量打印
			LOGGER.debug("Unsupported charset {} for input string {}", toCharset, input, e);
			return null;
		}
	}
}

