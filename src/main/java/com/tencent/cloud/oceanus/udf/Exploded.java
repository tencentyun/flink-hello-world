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
