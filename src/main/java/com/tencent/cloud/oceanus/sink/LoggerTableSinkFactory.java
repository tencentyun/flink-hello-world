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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Logger table sink factory prints all input records using SLF4J loggers.
 * It prints both toString and JSON format in the TaskManager log files.
 */
@PublicEvolving
public class LoggerTableSinkFactory implements DynamicTableSinkFactory {

	public static final String IDENTIFIER = "logger";

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
		return new HashSet<>();
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		return new LoggerSink();
	}

	private static class LoggerSink implements DynamicTableSink {

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
			return SinkFunctionProvider.of(new Slf4jSink<>());
		}

		@Override
		public DynamicTableSink copy() {
			return new LoggerSink();
		}

		@Override
		public String asSummaryString() {
			return "Logger";
		}
	}
}

class Slf4jSink<T> implements SinkFunction<T> {
	private static final Logger LOGGER = LoggerFactory.getLogger(Slf4jSink.class);
	private static final ObjectMapper MAPPER = new ObjectMapper();

	private static final long serialVersionUID = 1L;

	@Override
	public void invoke(T value) {
		String jsonString = "";
		try {
			jsonString = MAPPER.writeValueAsString(value);
		} catch (JsonProcessingException e) {
			LOGGER.debug("Unable to serialize value into JSON", e);
		}
		LOGGER.info("toString: {}, JSON: {}", value.toString(), jsonString);
	}
}
