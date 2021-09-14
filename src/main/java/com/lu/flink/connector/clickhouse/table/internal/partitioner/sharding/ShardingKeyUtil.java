package com.lu.flink.connector.clickhouse.table.internal.partitioner.sharding;

import java.util.List;

public class ShardingKeyUtil {
    public static List<AbstractShardingKey> findShardingKeys() {
        return ShardingKeyService.find(AbstractShardingKey.class);
    }

    public static AbstractShardingKey findAndCreateShardingKey(String identifier) {
        return ShardingKeyService.find(AbstractShardingKey.class, identifier);
    }
}
