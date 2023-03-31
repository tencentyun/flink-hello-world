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

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * 生成多行整数.
 *
 * <p>SQL 代码声明方式: CREATE TEMPORARY SYSTEM FUNCTION Exploded AS
 * 'com.tencent.cloud.oceanus.udf.Exploded' LANGUAGE JAVA;
 *
 * <p>常用于数据倾斜时的数据打散，把小表数据复制 n 份。例如： CREATE VIEW v_small AS SELECT * FROM small, LATERAL
 * TABLE(Exploded(50)) as t(`random_index`);
 */
@FunctionHint(output = @DataTypeHint("ROW<exploded_id int>"))
public class Exploded extends TableFunction<Row> {
    public void eval(Integer n) {
        for (int i = 0; i < n; i++) {
            collect(Row.of(i));
        }
    }
}
