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
