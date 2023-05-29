package com.tencent.cloud.oceanus.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.RowKind;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import static com.tencent.cloud.oceanus.sink.LoggerTableSinkFactory.MUTE_OUTPUT;

/**
 * Logger table sink factory prints all input records using SLF4J loggers. It prints both toString
 * and JSON format in the TaskManager log files.
 */
@PublicEvolving
public class LoggerTableSinkFactory implements DynamicTableSinkFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerTableSinkFactory.class);

    public static final String IDENTIFIER = "logger";
    public static final ConfigOption<String> PRINT_IDENTIFIER =
            ConfigOptions.key("print-identifier")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Message that identify logger and is prefixed to the output of the value.");

    public static final ConfigOption<Boolean> ALL_CHANGELOG_MODE =
            ConfigOptions.key("all-changelog-mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to accept all changelog mode.");

    public static final ConfigOption<Integer> RECORDS_PER_SECOND =
            ConfigOptions.key("records-per-second")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "Control how many records are written to the sink per second. "
                                    + "Use -1 for unlimited output.");

    public static final ConfigOption<Boolean> MUTE_OUTPUT =
            ConfigOptions.key("mute-output")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to discard all incoming records (similar to blackhole sink).");

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
        optionalOptions.add(RECORDS_PER_SECOND);
        optionalOptions.add(MUTE_OUTPUT);
        return optionalOptions;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig options = helper.getOptions();
        helper.validate();
        Class<?> clazz = context.getClass();
        CatalogTable table = null;
        try {
            Method method = clazz.getDeclaredMethod("getCatalogTable");
            table =
                    (CatalogTable)
                            method.invoke(context); // to be compatible with older Flink versions
        } catch (Exception e) {
            LOGGER.error("Failed to get catalog table", e);
        }
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(table.getSchema());
        return new LoggerSink(
                table.getSchema().toRowDataType(),
                physicalSchema,
                options.get(PRINT_IDENTIFIER),
                options.get(RECORDS_PER_SECOND),
                options.get(ALL_CHANGELOG_MODE),
                options.get(MUTE_OUTPUT));
    }

    private static class LoggerSink implements DynamicTableSink {

        private final String printIdentifier;
        private final TableSchema tableSchema;
        private final boolean allChangeLogMode;
        private final DataType type;
        private final int recordsPerSecond;
        private final boolean muteOutput;

        public LoggerSink(
                DataType type,
                TableSchema tableSchema,
                String printIdentifier,
                int recordsPerSecond,
                boolean allChangeLogMode,
                boolean muteOutput) {
            this.type = type;
            this.printIdentifier = printIdentifier;
            this.tableSchema = tableSchema;
            this.recordsPerSecond = recordsPerSecond;
            this.allChangeLogMode = allChangeLogMode;
            this.muteOutput = muteOutput;
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
            Slf4jSink.Builder<RowData> builder =
                    Slf4jSink.<RowData>builder()
                            .setPrintIdentifier(printIdentifier)
                            .setRecordsPerSecond(recordsPerSecond)
                            .setConverter(converter)
                            .setMuteOutput(muteOutput);
            return SinkFunctionProvider.of(builder.build());
        }

        @Override
        public DynamicTableSink copy() {
            return new LoggerSink(
                    type,
                    tableSchema,
                    printIdentifier,
                    recordsPerSecond,
                    allChangeLogMode,
                    muteOutput);
        }

        @Override
        public String asSummaryString() {
            return "Logger";
        }
    }
}

@SuppressWarnings("UnstableApiUsage")
class Slf4jSink<T> extends RichSinkFunction<T> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(Slf4jSink.class);
    private final String printIdentifier;
    private final DynamicTableSink.DataStructureConverter converter;
    private final int recordsPerSecond;
    private final boolean muteOutput;
    private transient RateLimiter rateLimiter;
    private Counter numberOfInsertRecords;
    private Counter numberOfUpdateBeforeRecords;
    private Counter numberOfUpdateAfterRecords;
    private Counter numberOfDeleteRecords;

    public Slf4jSink(
            String printIdentifier,
            DynamicTableSink.DataStructureConverter converter,
            int recordsPerSecond,
            boolean muteOutput) {
        this.printIdentifier = printIdentifier;
        this.converter = converter;
        this.recordsPerSecond = recordsPerSecond;
        this.muteOutput = muteOutput;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        MetricGroup metricGroup = null;
        try {
            Method method = RuntimeContext.class.getDeclaredMethod("getMetricGroup");
            metricGroup =
                    (MetricGroup)
                            method.invoke(getRuntimeContext()); // to be compatible with older Flink
            // versions

            this.numberOfInsertRecords = metricGroup.counter("numberOfInsertRecords");
            this.numberOfUpdateBeforeRecords = metricGroup.counter("numberOfUpdateBeforeRecords");
            this.numberOfUpdateAfterRecords = metricGroup.counter("numberOfUpdateAfterRecords");
            this.numberOfDeleteRecords = metricGroup.counter("numberOfDeleteRecords");
        } catch (Exception e) {
            LOGGER.error("Failed to get metric group", e);
        }

        if (recordsPerSecond > 0) {
            LOGGER.info("Sink output rate is limited to {} records per second.", recordsPerSecond);
            rateLimiter = RateLimiter.create(recordsPerSecond);
        }

        if (muteOutput) {
            LOGGER.info("All records would be discarded because `{}` is set.", MUTE_OUTPUT.key());
        }
    }

    @Override
    public void invoke(T value, Context context) {
        if (rateLimiter != null) {
            rateLimiter.acquire();
        }

        markRecordCountByType(value);

        if (!muteOutput) {
            Object data = converter.toExternal(value);
            LOGGER.info("{}-toString: {}", printIdentifier, data);
        }
    }

    private void markRecordCountByType(T record) {
        if (!(record instanceof RowData)) {
            return;
        }

        if (numberOfInsertRecords == null) {
            return;
        }

        switch (((RowData) record).getRowKind()) {
            case INSERT:
                numberOfInsertRecords.inc();
                break;
            case UPDATE_BEFORE:
                numberOfUpdateBeforeRecords.inc();
                break;
            case UPDATE_AFTER:
                numberOfUpdateAfterRecords.inc();
                break;
            case DELETE:
                numberOfDeleteRecords.inc();
                break;
            default:
                LOGGER.debug("Unsupported row kind type {}", ((RowData) record).getRowKind());
                break;
        }
    }

    /**
     * A builder used to set parameters to the output format's configuration in a fluent way.
     *
     * @return builder
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder for {@link Slf4jSink}. */
    public static class Builder<T> {
        private String printIdentifier;
        private DynamicTableSink.DataStructureConverter converter;
        private int recordsPerSecond;
        private boolean muteOutput;

        public Builder() {}

        public Builder<T> setPrintIdentifier(String printIdentifier) {
            this.printIdentifier = printIdentifier;
            return this;
        }

        public Builder<T> setConverter(DynamicTableSink.DataStructureConverter converter) {
            this.converter = converter;
            return this;
        }

        public Builder<T> setRecordsPerSecond(int recordsPerSecond) {
            this.recordsPerSecond = recordsPerSecond;
            return this;
        }

        public Builder<T> setMuteOutput(boolean muteOutput) {
            this.muteOutput = muteOutput;
            return this;
        }

        public Slf4jSink<T> build() {
            return new Slf4jSink<>(printIdentifier, converter, recordsPerSecond, muteOutput);
        }
    }
}
