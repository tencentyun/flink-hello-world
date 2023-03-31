/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
