package com.lu.flink.connector.clickhouse.table.internal.partitioner.sharding;

import org.apache.flink.table.data.RowData;

public interface ShardingKey {
    String identifier();
    void setGetter(RowData.FieldGetter getter);
    int getShardingValue(RowData rowData, RowData.FieldGetter getter);
}
