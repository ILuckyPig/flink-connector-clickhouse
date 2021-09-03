package com.lu.flink.connector.clickhouse.table.internal;


import com.lu.flink.connector.clickhouse.table.internal.convertor.ClickHouseRowConverter;
import com.lu.flink.connector.clickhouse.table.internal.executor.ClickHouseExecutor;
import com.lu.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * @author luanxin
 */
public abstract class AbstractClickHouseSinkFunction extends RichSinkFunction<RowData> implements Flushable {
    public static class Builder {
        private static final Logger LOG = LoggerFactory.getLogger(AbstractClickHouseSinkFunction.Builder.class);
        private DataType[] fieldDataTypes;
        private ClickHouseOptions options;
        private String[] fieldNames;
        private Optional<UniqueConstraint> primaryKey;
        private TypeInformation<RowData> rowDataTypeInformation;

        public AbstractClickHouseSinkFunction.Builder withOptions(ClickHouseOptions options) {
            this.options = options;
            return this;
        }

        public AbstractClickHouseSinkFunction.Builder withFieldDataTypes(DataType[] fieldDataTypes) {
            this.fieldDataTypes = fieldDataTypes;
            return this;
        }

        public AbstractClickHouseSinkFunction.Builder withFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public AbstractClickHouseSinkFunction.Builder withRowDataTypeInfo(TypeInformation<RowData> rowDataTypeInfo) {
            this.rowDataTypeInformation = rowDataTypeInfo;
            return this;
        }

        public AbstractClickHouseSinkFunction.Builder withPrimaryKey(Optional<UniqueConstraint> primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public AbstractClickHouseSinkFunction build() {
            Preconditions.checkNotNull(options);
            Preconditions.checkNotNull(fieldNames);
            Preconditions.checkNotNull(fieldDataTypes);
            LogicalType[] logicalTypes = Arrays.stream(fieldDataTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new);
            ClickHouseRowConverter converter = new ClickHouseRowConverter(RowType.of(logicalTypes));
            if (primaryKey.isPresent()) {
                LOG.warn("If primary key is specified, connector will be in UPSERT mode.");
                LOG.warn("You will have significant performance loss.");
            }

            return (AbstractClickHouseSinkFunction) (options.getWriteLocal() ? createShardOutputFormat(logicalTypes, converter) : createBatchSinkFunction(converter));
        }

        private ClickHouseBatchSinkFunction createBatchSinkFunction(ClickHouseRowConverter converter) {
            ClickHouseExecutor executor;
            if (primaryKey.isPresent() && !options.getIgnoreDelete()) {
                executor = ClickHouseExecutor.createUpsertExecutor(
                        options.getTableName(),
                        fieldNames,
                        listToStringArray(primaryKey.get().getColumns()),
                        converter,
                        options);
            } else {
                String sql = ClickHouseStatementFactory.getInsertIntoStatement(options.getTableName(), fieldNames);
                executor = new ClickHouseBatchExecutor(
                        sql,
                        converter,
                        options.getFlushInterval(),
                        options.getBatchSize(),
                        options.getMaxRetries(),
                        rowDataTypeInformation);
            }

            return new ClickHouseBatchSinkFunction(new ClickHouseConnectionProvider(options), (ClickHouseExecutor) executor, options);
        }

        private ClickHouseShardOutputFormat createShardOutputFormat(LogicalType[] logicalTypes, ClickHouseRowConverter converter) {
            String partitionStrategy = options.getPartitionStrategy();
            byte var5 = -1;
            switch (partitionStrategy) {
                case -1924829944:
                    if (var4.equals("balanced")) {
                        var5 = 0;
                    }
                    break;
                case 3195150:
                    if (var4.equals("hash")) {
                        var5 = 2;
                    }
                    break;
                case 2072332025:
                    if (var4.equals("shuffle")) {
                        var5 = 1;
                    }
            }

            ClickHousePartitioner partitioner;
            switch (var5) {
                case 0:
                    partitioner = ClickHousePartitioner.createBalanced();
                    break;
                case 1:
                    partitioner = ClickHousePartitioner.createShuffle();
                    break;
                case 2:
                    int index = Arrays.asList(fieldNames).indexOf(options.getPartitionKey());
                    if (index == -1) {
                        throw new IllegalArgumentException("Partition key `" + options.getPartitionKey() + "` not found in table schema");
                    }

                    RowData.FieldGetter getter = RowData.createFieldGetter(logicalTypes[index], index);
                    partitioner = ClickHousePartitioner.createHash(getter);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown sink.partition-strategy `" + options.getPartitionStrategy() + "`");
            }

            Optional keyFields;
            if (primaryKey.isPresent() && !options.getIgnoreDelete()) {
                keyFields = Optional.of(listToStringArray(((UniqueConstraint) primaryKey.get()).getColumns()));
            } else {
                keyFields = Optional.empty();
            }

            return new ClickHouseShardOutputFormat(new ClickHouseConnectionProvider(options), fieldNames, keyFields, converter, partitioner, options);
        }

        private String[] listToStringArray(List<String> lists) {
            if (lists == null) {
                return new String[0];
            }

            String[] keyFields = new String[lists.size()];
            int i = 0;
            for (String s : lists) {
                keyFields[i++] = s;
            }
            return keyFields;

        }
    }
}
