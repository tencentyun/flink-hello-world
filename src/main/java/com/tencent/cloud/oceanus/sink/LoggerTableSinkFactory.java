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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
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

	public static final ConfigOption<Boolean> ALL_CHANGELOG_MODE = ConfigOptions
			.key("all-changelog-mode")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether to accept all changelog mode.");

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
		optionalOptions.add(ALL_CHANGELOG_MODE);
		return optionalOptions;
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig options = helper.getOptions();
		helper.validate();
		Class<?> clazz = context.getClass();
		Method method;
		CatalogTable table = null;
		try {
			method = clazz.getDeclaredMethod("getCatalogTable");
			method.setAccessible(true);
			table = (CatalogTable) method.invoke(context);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		TableSchema physicalSchema =
			TableSchemaUtils.getPhysicalSchema(table.getSchema());
		return new LoggerSink(
				table.getSchema().toRowDataType(),
				physicalSchema,
				options.get(PRINT_IDENTIFIER),
				options.get(ALL_CHANGELOG_MODE));
	}

	private static class LoggerSink implements DynamicTableSink {

		private final String printIdentifier;
		private final TableSchema tableSchema;
		private final boolean allChangeLogMode;
		private final DataType type;

		public LoggerSink(DataType type, TableSchema tableSchema, String printIdentifier, boolean allChangeLogMode) {
			this.type = type;
			this.printIdentifier = printIdentifier;
			this.tableSchema = tableSchema;
			this.allChangeLogMode = allChangeLogMode;
		}

		@Override
		public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
			if (allChangeLogMode) {
				return ChangelogMode.all();
			}

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
			DataStructureConverter converter = context.createDataStructureConverter(type);
			Slf4jSink.Builder<RowData> builder = Slf4jSink.<RowData>builder()
					.setFieldDataTypes(tableSchema.getFieldDataTypes())
					.setPrintIdentifier(printIdentifier)
					.setConverter(converter);
			return SinkFunctionProvider.of(builder.build());
		}

		@Override
		public DynamicTableSink copy() {
			return new LoggerSink(type, tableSchema, printIdentifier, allChangeLogMode);
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
	private final DynamicTableSink.DataStructureConverter converter;

	public Slf4jSink(String printIdentifier,
	                 LogicalType[] logicalTypes,
	                 DynamicTableSink.DataStructureConverter converter) {
		this.printIdentifier = printIdentifier;
		this.fieldGetters = new RowData.FieldGetter[logicalTypes.length];
		for (int i = 0; i < logicalTypes.length; i++) {
			fieldGetters[i] = createFieldGetter(logicalTypes[i], i);
		}
		this.converter = converter;
	}

	@Override
	public void invoke(T value, Context context) {
		Object data = converter.toExternal(value);
		StringBuilder builder = new StringBuilder();
		builder.append(printIdentifier);
		builder.append("-toString: ");
		builder.append(data);
		LOGGER.info(builder.toString());
	}

	/**
	 * A builder used to set parameters to the output format's configuration in a fluent way.
	 *
	 * @return builder
	 */
	public static <T> Builder<T> builder() {
		return new Builder<>();
	}

	/**
	 * Builder for {@link Slf4jSink}.
	 */
	public static class Builder<T> {
		private String printIdentifier;
		private DataType[] fieldDataTypes;
		private DynamicTableSink.DataStructureConverter converter;

		public Builder() {
		}

		public Builder<T> setFieldDataTypes(DataType[] fieldDataTypes) {
			this.fieldDataTypes = fieldDataTypes;
			return this;
		}

		public Builder<T> setPrintIdentifier(String printIdentifier) {
			this.printIdentifier = printIdentifier;
			return this;
		}

		public Builder<T> setConverter(DynamicTableSink.DataStructureConverter converter) {
			this.converter = converter;
			return this;
		}

		public Slf4jSink<T> build() {
			final LogicalType[] logicalTypes =
				Arrays.stream(fieldDataTypes)
					.map(DataType::getLogicalType)
					.toArray(LogicalType[]::new);
			return new Slf4jSink<>(printIdentifier, logicalTypes, converter);
		}
	}
}