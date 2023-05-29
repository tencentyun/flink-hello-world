package com.tencent.cloud.oceanus.udf;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/**
 * ARRAY_AGG 示例：数据类型为 bigint. 使用方法：{@code CREATE TEMPORARY SYSTEM FUNCTION ARRAY_AGG AS
 * 'com.tencent.cloud.oceanus.udf.IntArrayAgg' LANGUAGE JAVA;}
 */
public class IntArrayAgg extends ArrayAgg<Long> {
    @Override
    public DataType getElementDataType() {
        return DataTypes.BIGINT();
    }
}
