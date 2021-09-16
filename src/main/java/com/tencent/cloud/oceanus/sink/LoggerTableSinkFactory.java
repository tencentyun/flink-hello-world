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

package com.tencent.cloud.oceanus.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.data.RowData.createFieldGetter;

/**
 * Logger table sink factory prints all input records using SLF4J loggers.
 * It prints both toString and JSON format in the TaskManager log files.
 */
@PublicEvolving
public class LoggerTableSinkFactory implements DynamicTableSinkFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerTableSinkFactory.class);

    public static final String IDENTIFIER = "logger";
    public static final ConfigOption<String> PRINT_IDENTIFIER = ConfigOptions
        .key("print-identifier")
        .stringType()
        .defaultValue("")
        .withDescription("Message that identify logger and is prefixed to the output of the value.");
    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(PRINT_IDENTIFIER);
        return optionalOptions;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig options = helper.getOptions();
        helper.validate();
        Class clazz = context.getClass();
        Method m1 = null;
        CatalogTable table = null;
        try {
            m1 = clazz.getDeclaredMethod("getCatalogTable");
            m1.setAccessible(true);
            table = (CatalogTable) m1.invoke(context);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        TableSchema physicalSchema =
            TableSchemaUtils.getPhysicalSchema(table.getSchema());
        return new LoggerSink(options.get(PRINT_IDENTIFIER), physicalSchema);
    }

    private static class LoggerSink implements DynamicTableSink {

        private final String printIdentifier;
        private final TableSchema tableSchema;

        public LoggerSink(String printIdentifier, TableSchema tableSchema) {
            this.printIdentifier = printIdentifier;
            this.tableSchema = tableSchema;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            ChangelogMode.Builder builder = ChangelogMode.newBuilder();
            for (RowKind kind : requestedMode.getContainedKinds()) {
                if (kind != RowKind.UPDATE_BEFORE) {
                    builder.addContainedKind(kind);
                }
            }
            return builder.build();
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            Slf4jSink.Builder build = Slf4jSink.builder().setFieldDataTypes(tableSchema.getFieldDataTypes()).setPrintIdentifier(printIdentifier);
            return SinkFunctionProvider.of(build.build());
        }

        @Override
        public DynamicTableSink copy() {
            return new LoggerSink(printIdentifier, tableSchema);
        }

        @Override
        public String asSummaryString() {
            return "Logger";
        }
    }
}

class Slf4jSink<T> implements SinkFunction<T> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(Slf4jSink.class);
    private final String printIdentifier;
    private final RowData.FieldGetter[] fieldGetters;
    private static final String NULL_VALUE = "null";


    public Slf4jSink(String printIdentifier, LogicalType[] logicalTypes) {
        this.printIdentifier = printIdentifier;
        this.fieldGetters = new RowData.FieldGetter[logicalTypes.length];
        for (int i = 0; i < logicalTypes.length; i++) {
            fieldGetters[i] = createFieldGetter(logicalTypes[i], i);
        }
    }

    public String toString(RowData row) {
        StringBuilder sb = new StringBuilder();
        sb.append(row.getRowKind().shortString()).append("(");

        for (int i = 0; i < row.getArity() && i < fieldGetters.length; ++i) {
            if (i != 0) {
                sb.append(",");
            }
            Object field = fieldGetters[i].getFieldOrNull(row);
            if (field == null) {
                field = NULL_VALUE;
            }
            sb.append(StringUtils.arrayAwareToString(field));
        }

        sb.append(")");
        return sb.toString();
    }

    @Override
    public void invoke(T value, Context context) {
        StringBuilder builder = new StringBuilder();
        builder.append("====== " + printIdentifier);
        RowData row = (RowData)value;
        builder.append("-toString: ");
        builder.append(toString(row));
        LOGGER.info(builder.toString());
    }

    /**
     * A builder used to set parameters to the output format's configuration in a fluent way.
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link Slf4jSink}. */
    public static class Builder {
        private String printIdentifier;
        private DataType[] fieldDataTypes;

        public Builder() {
        }

        public Builder setFieldDataTypes(DataType[] fieldDataTypes) {
            this.fieldDataTypes = fieldDataTypes;
            return this;
        }

        public Builder setPrintIdentifier(String printIdentifier) {
            this.printIdentifier = printIdentifier;
            return this;
        }

        public Slf4jSink build() {
            final LogicalType[] logicalTypes =
                Arrays.stream(fieldDataTypes)
                    .map(DataType::getLogicalType)
                    .toArray(LogicalType[]::new);
            return new Slf4jSink(printIdentifier, logicalTypes);
        }
    }
}