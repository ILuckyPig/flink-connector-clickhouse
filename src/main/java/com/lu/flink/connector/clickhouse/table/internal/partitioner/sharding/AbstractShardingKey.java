package com.lu.flink.connector.clickhouse.table.internal.partitioner.sharding;

import com.lu.flink.connector.clickhouse.table.internal.partitioner.ClickHousePartitioner;
import org.apache.flink.table.data.RowData;

public abstract class AbstractShardingKey implements ShardingKey, ClickHousePartitioner {
    private RowData.FieldGetter getter;

    public AbstractShardingKey() {
    }

    public AbstractShardingKey(RowData.FieldGetter getter) {
        this.getter = getter;
    }

    public RowData.FieldGetter getGetter() {
        return getter;
    }

    public void setGetter(RowData.FieldGetter getter) {
        this.getter = getter;
    }

    @Override
    public int select(RowData record, int numShards) {
        return getShardingValue(record, getter) % numShards;
    }
}
