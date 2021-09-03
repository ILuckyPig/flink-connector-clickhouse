package com.lu.flink.connector.clickhouse.table;

import com.lu.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

public class ClickHouseDynamicTableSink implements DynamicTableSink {
    private final ClickHouseOptions clickHouseOptions;
    private final ResolvedSchema resolvedSchema;

    public ClickHouseDynamicTableSink(ClickHouseOptions clickHouseOptions, ResolvedSchema resolvedSchema) {
        this.clickHouseOptions = clickHouseOptions;
        this.resolvedSchema = resolvedSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        validatePrimaryKey(changelogMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        Preconditions.checkState(ChangelogMode.insertOnly().equals(requestedMode) || resolvedSchema.getPrimaryKey().isPresent(),
                "please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // TODO ClickHouseTableSinkFunction
        return OutputFormatProvider.of(null);
    }

    @Override
    public DynamicTableSink copy() {
        return new ClickHouseDynamicTableSink(clickHouseOptions, resolvedSchema);
    }

    @Override
    public String asSummaryString() {
        return "ClickHouseDynamicSink";
    }
}
