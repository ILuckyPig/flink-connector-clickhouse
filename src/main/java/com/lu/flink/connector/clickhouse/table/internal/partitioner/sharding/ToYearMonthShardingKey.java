package com.lu.flink.connector.clickhouse.table.internal.partitioner.sharding;

import org.apache.flink.table.data.RowData;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class ToYearMonthShardingKey extends AbstractShardingKey {
    private static final String IDENTIFIER = "toYYYYMM";
    private static final DateTimeFormatter YEAR_MONTH_FORMATTER = DateTimeFormatter.ofPattern("yyyyMM");

    public ToYearMonthShardingKey() {

    }

    public ToYearMonthShardingKey(RowData.FieldGetter getter) {
        super(getter);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public int getShardingValue(RowData rowData, RowData.FieldGetter getter) {
        int epochDay = (int) getter.getFieldOrNull(rowData);
        LocalDate localDate = LocalDate.ofEpochDay(epochDay);
        return Integer.parseInt(localDate.format(YEAR_MONTH_FORMATTER));
    }
}
