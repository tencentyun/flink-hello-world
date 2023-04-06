package com.tencent.cloud.oceanus.udf;

import org.apache.flink.table.functions.ScalarFunction;

import org.apache.commons.lang3.StringUtils;

/**
 * 将 str 表示的字符串以 separator 指定的分隔符拆分，返回值拆分后的数组.
 *
 * <p>SQL 代码声明方式:：CREATE TEMPORARY SYSTEM FUNCTION STR_TO_ARRAY AS
 * 'com.tencent.cloud.oceanus.udf.StrToArray' LANGUAGE JAVA;
 *
 * <p>注意相邻的分割符拆分出空字符串，例如 SELECT STR_TO_ARRAY('a,b,,c,d', ','); 返回 [a, b, , c, d].
 */
public class StrToArray extends ScalarFunction {
    public String[] eval(String str, String separator) {
        if (str == null) {
            return null;
        }
        return StringUtils.splitByWholeSeparatorPreserveAllTokens(str, separator);
    }
}
