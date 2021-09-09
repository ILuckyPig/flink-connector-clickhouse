package com.lu.flink.connector.clickhouse.table.internal.executor;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.lu.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.lu.flink.connector.clickhouse.table.internal.convertor.ClickHouseRowConverter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class ClickHouseBatchExecutor implements ClickHouseExecutor {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchExecutor.class);
    private transient ClickHousePreparedStatement stmt;
    private transient ClickHouseConnectionProvider connectionProvider;
    private RuntimeContext context;
    private final TypeInformation<RowData> rowDataTypeInformation;
    private final String sql;
    private final ClickHouseRowConverter converter;
    private transient List<RowData> batch;
    private final Duration flushInterval;
    private final int batchSize;
    private final int maxRetries;
    private transient TypeSerializer<RowData> typeSerializer;
    private boolean objectReuseEnabled = false;
    private transient ClickHouseBatchExecutor.ExecuteBatchService service;

    public ClickHouseBatchExecutor(String sql, ClickHouseRowConverter converter, Duration flushInterval, int batchSize,
                                   int maxRetries, TypeInformation<RowData> rowDataTypeInformation) {
        this.sql = sql;
        this.converter = converter;
        this.flushInterval = flushInterval;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.rowDataTypeInformation = rowDataTypeInformation;
    }

    public void prepareStatement(ClickHouseConnection connection) throws SQLException {
        this.batch = new ArrayList<>();
        this.stmt = (ClickHousePreparedStatement) connection.prepareStatement(this.sql);
        this.service = new ClickHouseBatchExecutor.ExecuteBatchService();
        this.service.startAsync();
    }

    public void prepareStatement(ClickHouseConnectionProvider connectionProvider) throws SQLException {
        this.connectionProvider = connectionProvider;
        this.batch = new ArrayList<>();
        this.stmt = (ClickHousePreparedStatement) connectionProvider.getConnection().prepareStatement(this.sql);
    }

    public void setRuntimeContext(RuntimeContext context) {
        this.context = context;
        this.typeSerializer = this.rowDataTypeInformation.createSerializer(context.getExecutionConfig());
        this.objectReuseEnabled = context.getExecutionConfig().isObjectReuseEnabled();
    }

    public synchronized void addBatch(RowData record) throws IOException {
        if (record.getRowKind() != RowKind.DELETE && record.getRowKind() != RowKind.UPDATE_BEFORE) {
            if (this.objectReuseEnabled) {
                this.batch.add(this.typeSerializer.copy(record));
            } else {
                this.batch.add(record);
            }
        }

    }

    public synchronized void executeBatch() throws IOException {
        if (this.service.isRunning()) {
            this.notifyAll();
        } else {
            throw new IOException("executor unexpectedly terminated", this.service.failureCause());
        }
    }

    private void attemptExecuteBatch() throws IOException {
        int i = 1;

        while (i <= this.maxRetries) {
            try {
                this.stmt.executeBatch();
                this.stmt.clearBatch();
                this.batch.clear();
                break;
            } catch (SQLException sqlException) {
                LOG.error("ClickHouse executeBatch error, retry times = {}", i, sqlException);
                if (i >= this.maxRetries) {
                    throw new IOException(sqlException);
                }

                try {
                    Thread.sleep(1000L * i);
                } catch (InterruptedException var4) {
                    Thread.currentThread().interrupt();
                    throw new IOException("unable to flush; interrupted while doing another attempt", sqlException);
                }

                i++;
            }
        }
    }

    public void closeStatement() throws SQLException {
        if (this.service != null) {
            this.service.stopAsync().awaitTerminated();
        } else {
            LOG.warn("executor closed before initialized");
        }

        if (this.stmt != null) {
            this.stmt.close();
            this.stmt = null;
        }
    }

    private class ExecuteBatchService extends AbstractExecutionThreadService {
        private ExecuteBatchService() {
        }

        protected void run() throws Exception {
            while (this.isRunning()) {
                synchronized (ClickHouseBatchExecutor.this) {
                    ClickHouseBatchExecutor.this.wait(ClickHouseBatchExecutor.this.flushInterval.toMillis());
                    if (!ClickHouseBatchExecutor.this.batch.isEmpty()) {
                        for (RowData r : ClickHouseBatchExecutor.this.batch) {
                            ClickHouseBatchExecutor.this.converter.toClickHouse(r, ClickHouseBatchExecutor.this.stmt);
                            ClickHouseBatchExecutor.this.stmt.addBatch();
                        }

                        this.attemptExecuteBatch();
                    }
                }
            }

        }

        private void attemptExecuteBatch() throws IOException {
            int i = 1;

            while (i <= ClickHouseBatchExecutor.this.maxRetries) {
                try {
                    ClickHouseBatchExecutor.this.stmt.executeBatch();
                    ClickHouseBatchExecutor.this.batch.clear();
                    break;
                } catch (SQLException sqlException) {
                    ClickHouseBatchExecutor.LOG.error("ClickHouse executeBatch error, retry times = {}", i, sqlException);
                    if (i >= ClickHouseBatchExecutor.this.maxRetries) {
                        throw new IOException(sqlException);
                    }

                    try {
                        Thread.sleep(1000L * i);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("unable to flush; interrupted while doing another attempt", sqlException);
                    }

                    i++;
                }
            }
        }
    }
}
