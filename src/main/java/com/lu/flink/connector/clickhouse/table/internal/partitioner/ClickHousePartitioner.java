package com.lu.flink.connector.clickhouse.table.internal.partitioner;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;

public interface ClickHousePartitioner extends Serializable {
    String BALANCED = "balanced";
    String SHUFFLE = "shuffle";
    String HASH = "hash";

    int select(RowData record, int numShards);

    static ClickHousePartitioner createBalanced() {
        return new BalancedPartitioner();
    }

    static ClickHousePartitioner createShuffle() {
        return new ShufflePartitioner();
    }

    static ClickHousePartitioner createHash(RowData.FieldGetter getter) {
        return new HashPartitioner(getter);
    }
}
