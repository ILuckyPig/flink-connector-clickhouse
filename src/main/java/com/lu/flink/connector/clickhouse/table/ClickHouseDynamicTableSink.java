package com.lu.flink.connector.clickhouse.table;

import com.lu.flink.connector.clickhouse.table.internal.AbstractClickHouseSinkFunction;
import com.lu.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

public class ClickHouseDynamicTableSink implements DynamicTableSink {
    private final ClickHouseOptions options;
    private final ResolvedSchema resolvedSchema;

    public ClickHouseDynamicTableSink(ClickHouseOptions options, ResolvedSchema resolvedSchema) {
        this.options = options;
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
        AbstractClickHouseSinkFunction sinkFunction = (new AbstractClickHouseSinkFunction.Builder())
                .withOptions(this.options)
                .withFieldNames(this.resolvedSchema.getColumnNames().toArray(new String[0]))
                .withFieldDataTypes(this.resolvedSchema.getColumnDataTypes().toArray(new DataType[0]))
                .withPrimaryKey(this.resolvedSchema.getPrimaryKey())
                .withRowDataTypeInfo(context.createTypeInformation(this.resolvedSchema.toSourceRowDataType()))
                .build();
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new ClickHouseDynamicTableSink(options, resolvedSchema);
    }

    @Override
    public String asSummaryString() {
        return "ClickHouseDynamicSink";
    }
}
