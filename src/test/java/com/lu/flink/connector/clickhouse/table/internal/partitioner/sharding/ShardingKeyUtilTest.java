package com.lu.flink.connector.clickhouse.table.internal.partitioner.sharding;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DateType;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.util.List;

public class ShardingKeyUtilTest {
    @Test
    public void test() {
        List<AbstractShardingKey> shardingKeys = ShardingKeyUtil.findShardingKeys();
        Assert.assertFalse(shardingKeys.isEmpty());
        for (ShardingKey shardingKey : shardingKeys) {
            System.out.println(shardingKey.identifier());
        }
    }

    @Test
    public void testCreate() {
        AbstractShardingKey shardingKey = ShardingKeyUtil.findAndCreateShardingKey("toYYYYMM");
        System.out.println(shardingKey.identifier());
    }

    @Test
    public void testSetGetter() {
        AbstractShardingKey shardingKey = ShardingKeyUtil.findAndCreateShardingKey("toYYYYMM");
        System.out.println(shardingKey.identifier());

        long toEpochDay = LocalDate.parse("2021-09-05").toEpochDay();
        System.out.println(toEpochDay);

        RowData.FieldGetter fieldGetter = RowData.createFieldGetter(new DateType(false), 0);
        GenericRowData genericRowData = new GenericRowData(1);
        genericRowData.setField(0, (int) toEpochDay);

        shardingKey.setGetter(fieldGetter);

        int shardingValue = shardingKey.getShardingValue(genericRowData, fieldGetter);
        System.out.println(shardingValue);
        Assert.assertEquals(202109, shardingValue);
        int select = shardingKey.select(genericRowData, 2);
        System.out.println(select);
        Assert.assertEquals(202109 % 2, select);
    }
}
