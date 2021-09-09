package com.lu.flink.connector.clickhouse.table.internal.executor;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.lu.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.lu.flink.connector.clickhouse.table.internal.convertor.ClickHouseRowConverter;
import com.lu.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ClickHouseUpsertExecutor implements ClickHouseExecutor {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseUpsertExecutor.class);
    private transient ClickHousePreparedStatement insertStmt;
    private transient ClickHousePreparedStatement updateStmt;
    private transient ClickHousePreparedStatement deleteStmt;
    private final String insertSql;
    private final String updateSql;
    private final String deleteSql;
    private final ClickHouseRowConverter converter;
    private final transient List<RowData> insertBatch;
    private final transient List<RowData> updateBatch;
    private final transient List<RowData> deleteBatch;
    private transient ClickHouseUpsertExecutor.ExecuteBatchService service;
    private final Duration flushInterval;
    private final int maxRetries;

    public ClickHouseUpsertExecutor(String insertSql, String updateSql, String deleteSql, ClickHouseRowConverter converter, ClickHouseOptions options) {
        this.insertSql = insertSql;
        this.updateSql = updateSql;
        this.deleteSql = deleteSql;
        this.converter = converter;
        this.flushInterval = options.getFlushInterval();
        this.maxRetries = options.getMaxRetries();
        this.insertBatch = new ArrayList<>();
        this.updateBatch = new ArrayList<>();
        this.deleteBatch = new ArrayList<>();
    }

    public void prepareStatement(ClickHouseConnection connection) throws SQLException {
        this.insertStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.insertSql);
        this.updateStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.updateSql);
        this.deleteStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.deleteSql);
        this.service = new ExecuteBatchService();
        this.service.startAsync();
    }

    public void prepareStatement(ClickHouseConnectionProvider connectionProvider) throws SQLException {
    }

    public void setRuntimeContext(RuntimeContext context) {
    }

    public synchronized void addBatch(RowData record) {
        switch (record.getRowKind()) {
            case INSERT:
                this.insertBatch.add(record);
                break;
            case UPDATE_AFTER:
                this.updateBatch.add(record);
                break;
            case DELETE:
                this.deleteBatch.add(record);
            case UPDATE_BEFORE:
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE, but get: %s.", record.getRowKind()));
        }

    }

    public synchronized void executeBatch() throws IOException {
        if (this.service.isRunning()) {
            this.notifyAll();
        } else {
            throw new IOException("executor unexpectedly terminated", this.service.failureCause());
        }
    }

    public void closeStatement() throws SQLException {
        if (this.service != null) {
            this.service.stopAsync().awaitTerminated();
        } else {
            LOG.warn("executor closed before initialized");
        }

        for (ClickHousePreparedStatement clickHousePreparedStatement : Arrays.asList(this.insertStmt, this.updateStmt, this.deleteStmt)) {
            if (clickHousePreparedStatement != null) {
                clickHousePreparedStatement.close();
            }
        }

    }

    private class ExecuteBatchService extends AbstractExecutionThreadService {
        private ExecuteBatchService() {
        }

        protected void run() throws Exception {
            while (this.isRunning()) {
                synchronized (ClickHouseUpsertExecutor.this) {
                    ClickHouseUpsertExecutor.this.wait(ClickHouseUpsertExecutor.this.flushInterval.toMillis());
                    this.processBatch(ClickHouseUpsertExecutor.this.insertStmt, ClickHouseUpsertExecutor.this.insertBatch);
                    this.processBatch(ClickHouseUpsertExecutor.this.updateStmt, ClickHouseUpsertExecutor.this.updateBatch);
                    this.processBatch(ClickHouseUpsertExecutor.this.deleteStmt, ClickHouseUpsertExecutor.this.deleteBatch);
                }
            }

        }

        private void processBatch(ClickHousePreparedStatement stmt, List<RowData> batch) throws SQLException, IOException {
            if (!batch.isEmpty()) {
                for (RowData rowData : ClickHouseUpsertExecutor.this.insertBatch) {
                    ClickHouseUpsertExecutor.this.converter.toClickHouse(rowData, stmt);
                    stmt.addBatch();
                }

                this.attemptExecuteBatch(stmt, batch);
            }

        }

        private void attemptExecuteBatch(ClickHousePreparedStatement stmt, List<RowData> batch) throws IOException {
            int i = 1;

            while (i <= ClickHouseUpsertExecutor.this.maxRetries) {
                try {
                    stmt.executeBatch();
                    batch.clear();
                    break;
                } catch (SQLException sqlException) {
                    ClickHouseUpsertExecutor.LOG.error("ClickHouse executeBatch error, retry times = {}", i, sqlException);
                    if (i >= ClickHouseUpsertExecutor.this.maxRetries) {
                        throw new IOException(sqlException);
                    }

                    try {
                        Thread.sleep(1000L * i);
                    } catch (InterruptedException var6) {
                        Thread.currentThread().interrupt();
                        throw new IOException("unable to flush; interrupted while doing another attempt", sqlException);
                    }

                    i++;
                }
            }
        }
    }
}
