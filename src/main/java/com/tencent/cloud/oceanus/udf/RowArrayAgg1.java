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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * ARRAY_AGG 示例：数据类型为 {@code Row<f1 bigint, f2 string>}. 使用方法：{@code CREATE TEMPORARY SYSTEM
 * FUNCTION ARRAY_AGG AS 'com.tencent.cloud.oceanus.udf.RowArrayAgg1' LANGUAGE JAVA;}
 */
public class RowArrayAgg1 extends ArrayAgg<RowData> {
    @Override
    public DataType getElementDataType() {
        return DataTypes.ROW(DataTypes.BIGINT(), DataTypes.STRING()).bridgedTo(RowData.class);
    }
}
