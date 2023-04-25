package com.tencent.cloud.oceanus.udf;

import org.apache.flink.table.functions.ScalarFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.stream.IntStream;

/**
 * 如果 JDBC 数据库的 VARCHAR 为 Latin1 (或 GBK 等) 编码 可以使用这个函数转换为标准字符串
 *
 * <p>SQL 代码声明方式: CREATE TEMPORARY SYSTEM FUNCTION DECODE_LATIN1 AS
 * 'com.tencent.cloud.oceanus.udf.DecodeLatin1' LANGUAGE JAVA;
 */
public class DecodeLatin1 extends ScalarFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(DecodeLatin1.class);

    public String eval(String input) {
        return eval(input, "latin1");
    }

    public String eval(String input, String fromCharset) {
        char[] inputCharArray = input.toCharArray();

        // JDBC Driver 读取的 Latin1 字符, 高 8 位都是 0x00, 因此只考虑低 8 位即可, byte 和 char 数据部分等长, 长度无需乘以二
        byte[] inputBytes = new byte[inputCharArray.length];

        IntStream.range(0, inputCharArray.length)
                .forEach(
                        i -> {
                            inputBytes[i] = (byte) inputCharArray[i];
                            LOGGER.debug("{}", String.format("0x%02X ", inputBytes[i]));
                        });

        try {
            return new String(inputBytes, fromCharset);
        } catch (UnsupportedEncodingException e) {
            // 与 GET_JSON_OBJECT 的异常处理方式保持一致, 遇到异常数据时输出 null, 避免日志过量打印
            LOGGER.debug("Unsupported charset {} for input string {}", fromCharset, input, e);
            return null;
        }
    }
}
