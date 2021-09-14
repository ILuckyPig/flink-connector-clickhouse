package com.lu.flink.connector.clickhouse.table.internal.partitioner.sharding;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DateType;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;

public class ToYearMonthShardingKeyTest {
    @Test
    public void test() throws Exception {
        long toEpochDay = LocalDate.parse("2021-09-05").toEpochDay();
        System.out.println(toEpochDay);
        RowData.FieldGetter fieldGetter = RowData.createFieldGetter(new DateType(false), 0);
        GenericRowData genericRowData = new GenericRowData(1);
        genericRowData.setField(0, (int) toEpochDay);
        ToYearMonthShardingKey shardingKey = new ToYearMonthShardingKey(fieldGetter);
        int shardingValue = shardingKey.getShardingValue(genericRowData, shardingKey.getGetter());
        System.out.println(shardingValue);
        Assert.assertEquals(202109, shardingValue);
        int select = shardingKey.select(genericRowData, 2);
        System.out.println(select);
        Assert.assertEquals(202109 % 2, select);
    }
}
