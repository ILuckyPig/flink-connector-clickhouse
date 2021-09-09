package com.lu.flink.connector.clickhouse.table.internal;

import com.lu.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.lu.flink.connector.clickhouse.table.internal.convertor.ClickHouseRowConverter;
import com.lu.flink.connector.clickhouse.table.internal.executor.ClickHouseBatchExecutor;
import com.lu.flink.connector.clickhouse.table.internal.executor.ClickHouseExecutor;
import com.lu.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import com.lu.flink.connector.clickhouse.table.internal.partitioner.ClickHousePartitioner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClickHouseShardSinkFunction extends AbstractClickHouseSinkFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseShardSinkFunction.class);
    private static final Pattern PATTERN = Pattern.compile(
            "Distributed\\('?(?<cluster>[a-zA-Z_][0-9a-zA-Z_]*)'?," +
                    "\\s*'?(?<database>[a-zA-Z_][0-9a-zA-Z_]*)'?," +
                    "\\s*'?(?<table>[a-zA-Z_][0-9a-zA-Z_]*)'?");
    private final ClickHouseConnectionProvider connectionProvider;
    private final ClickHouseRowConverter converter;
    private final ClickHousePartitioner partitioner;
    private final ClickHouseOptions options;
    private final String[] fieldNames;
    private transient boolean closed = false;
    private transient ClickHouseConnection connection;
    private String remoteTable;
    private transient List<ClickHouseConnection> shardConnections;
    private transient int[] batchCounts;
    private final List<ClickHouseExecutor> shardExecutors;
    private final String[] keyFields;
    private final boolean ignoreDelete;

    protected ClickHouseShardSinkFunction(@Nonnull ClickHouseConnectionProvider connectionProvider, @Nonnull String[] fieldNames,
                                          @Nonnull Optional<String[]> keyFields, @Nonnull ClickHouseRowConverter converter,
                                          @Nonnull ClickHousePartitioner partitioner, @Nonnull ClickHouseOptions options) {
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.fieldNames = Preconditions.checkNotNull(fieldNames);
        this.converter = Preconditions.checkNotNull(converter);
        this.partitioner = Preconditions.checkNotNull(partitioner);
        this.options = Preconditions.checkNotNull(options);
        this.shardExecutors = new ArrayList<>();
        this.ignoreDelete = options.getIgnoreDelete();
        this.keyFields = keyFields.orElseGet(() -> new String[0]);

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            this.connection = this.connectionProvider.getConnection();
            this.establishShardConnections();
            this.initializeExecutors();
        } catch (Exception e) {
            throw new IOException("unable to establish connection to ClickHouse", e);
        }
    }

    private void establishShardConnections() throws IOException {
        try {
            String engine = this.connectionProvider.queryTableEngine(this.options.getDatabaseName(), this.options.getTableName());
            Matcher matcher = PATTERN.matcher(engine);
            if (matcher.find()) {
                String remoteCluster = matcher.group("cluster");
                String remoteDatabase = matcher.group("database");
                this.remoteTable = matcher.group("table");
                this.shardConnections = this.connectionProvider.getShardConnections(remoteCluster, remoteDatabase);
                this.batchCounts = new int[this.shardConnections.size()];
            } else {
                throw new IOException("table `" + this.options.getDatabaseName() + "`.`" + this.options.getTableName() + "` is not a Distributed table");
            }
        } catch (SQLException var5) {
            throw new IOException(var5);
        }
    }

    private void initializeExecutors() throws SQLException {
        String sql = ClickHouseStatementFactory.getInsertIntoStatement(this.remoteTable, this.fieldNames);

        for (int i = 0; i < this.shardConnections.size(); ++i) {
            ClickHouseExecutor executor;
            if (this.keyFields.length > 0) {
                executor = ClickHouseExecutor.createUpsertExecutor(this.remoteTable, this.fieldNames, this.keyFields, this.converter, this.options);
            } else {
                executor = new ClickHouseBatchExecutor(sql, this.converter, this.options.getFlushInterval(),
                        this.options.getBatchSize(), this.options.getMaxRetries(), null);
            }

            executor.prepareStatement(this.shardConnections.get(i));
            this.shardExecutors.add(executor);
        }

    }

    @Override
    public void invoke(RowData record, Context context) throws Exception {
        switch (record.getRowKind()) {
            case INSERT:
                this.writeRecordToOneExecutor(record);
                break;
            case UPDATE_AFTER:
                if (this.ignoreDelete) {
                    this.writeRecordToOneExecutor(record);
                } else {
                    this.writeRecordToAllExecutors(record);
                }
                break;
            case DELETE:
                if (!this.ignoreDelete) {
                    this.writeRecordToAllExecutors(record);
                }
            case UPDATE_BEFORE:
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE, but get: %s.", record.getRowKind()));
        }
    }

    private void writeRecordToOneExecutor(RowData record) throws IOException {
        int selected = this.partitioner.select(record, this.shardExecutors.size());
        this.shardExecutors.get(selected).addBatch(record);
        this.batchCounts[selected]++;
        if (this.batchCounts[selected] >= this.options.getBatchSize()) {
            this.flush(selected);
        }

    }

    private void writeRecordToAllExecutors(RowData record) throws IOException {
        for (int i = 0; i < this.shardExecutors.size(); ++i) {
            this.shardExecutors.get(i).addBatch(record);
            this.batchCounts[i]++;
            if (this.batchCounts[i] >= this.options.getBatchSize()) {
                this.flush(i);
            }
        }

    }

    public void flush() throws IOException {
        for (int i = 0; i < this.shardExecutors.size(); ++i) {
            this.flush(i);
        }

    }

    public void flush(int index) throws IOException {
        this.shardExecutors.get(index).executeBatch();
        this.batchCounts[index] = 0;
    }

    public void close() throws IOException {
        if (!this.closed) {
            this.closed = true;

            try {
                this.flush();
            } catch (Exception e) {
                LOG.warn("Writing records to ClickHouse failed.", e);
            }

            this.closeConnection();
        }

    }

    private void closeConnection() {
        if (this.connection != null) {
            try {
                for (ClickHouseExecutor shardExecutor : this.shardExecutors) {
                    shardExecutor.closeStatement();
                }

                this.connectionProvider.closeConnections();
            } catch (SQLException e) {
                LOG.warn("ClickHouse connection could not be closed: {}", e.getMessage());
            } finally {
                this.connection = null;
            }
        }

    }
}
