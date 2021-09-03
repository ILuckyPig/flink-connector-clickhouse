package com.lu.flink.connector.clickhouse.table.internal.executor;

import com.lu.flink.connector.clickhouse.table.internal.convertor.ClickHouseRowConverter;
import com.lu.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;

public interface ClickHouseExecutor {
    ClickHouseExecutor createUpsertExecutor(String tableName, String[] fieldNames, String[] listToStringArray, ClickHouseRowConverter converter, ClickHouseOptions options);
}
