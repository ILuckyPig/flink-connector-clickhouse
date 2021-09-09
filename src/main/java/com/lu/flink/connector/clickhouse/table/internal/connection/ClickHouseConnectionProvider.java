package com.lu.flink.connector.clickhouse.table.internal.connection;

import com.lu.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClickHouseConnectionProvider implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseConnectionProvider.class);
    private static final String CLICKHOUSE_DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver";
    private static final Pattern PATTERN = Pattern.compile("You must use port (?<port>[0-9]+) for HTTP.");
    private transient ClickHouseConnection connection;
    private transient List<ClickHouseConnection> shardConnections;
    private final ClickHouseOptions options;

    public ClickHouseConnectionProvider(ClickHouseOptions options) {
        this.options = options;
    }

    public synchronized ClickHouseConnection getConnection() throws SQLException {
        if (this.connection == null) {
            this.connection = this.createConnection(this.options.getUrl(), this.options.getDatabaseName());
        }
        return this.connection;
    }

    private ClickHouseConnection createConnection(String url, String database) throws SQLException {
        LOG.info("connecting to {}", url);

        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }

        ClickHouseConnection conn;
        if (this.options.getUsername().isPresent()) {
            conn = (ClickHouseConnection) DriverManager.getConnection(this.getJdbcUrl(url, database),
                    this.options.getUsername().orElse(null), this.options.getPassword().orElse(null));
        } else {
            conn = (ClickHouseConnection) DriverManager.getConnection(this.getJdbcUrl(url, database));
        }

        return conn;
    }

    public synchronized List<ClickHouseConnection> getShardConnections(String remoteCluster, String remoteDatabase) throws SQLException {
        if (this.shardConnections == null) {
            ClickHouseConnection conn = this.getConnection();
            PreparedStatement stmt = conn.prepareStatement("SELECT shard_num, host_address, port FROM system.clusters WHERE cluster = ?");
            stmt.setString(1, remoteCluster);
            try (ResultSet rs = stmt.executeQuery()) {

                this.shardConnections = new ArrayList<>();

                while (rs.next()) {
                    String host = rs.getString("host_address");
                    int port = this.getActualHttpPort(host, rs.getInt("port"));
                    String url = "clickhouse://" + host + ":" + port;
                    this.shardConnections.add(this.createConnection(url, remoteDatabase));
                }
            }
            if (this.shardConnections.isEmpty()) {
                throw new SQLException("unable to query shards in system.clusters");
            }
        }
        return this.shardConnections;
    }

    private int getActualHttpPort(String host, int port) throws SQLException {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet((new URIBuilder()).setScheme("http").setHost(host).setPort(port).build());
            CloseableHttpResponse response = httpclient.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                return port;
            }

            String raw = EntityUtils.toString(response.getEntity());
            Matcher matcher = PATTERN.matcher(raw);
            if (!matcher.find()) {
                throw new SQLException("Cannot query ClickHouse http port");
            }
            return Integer.parseInt(matcher.group("port"));
        } catch (Exception e) {
            throw new SQLException("Cannot connect to ClickHouse server using HTTP", e);
        }
    }

    public void closeConnections() throws SQLException {
        if (this.connection != null) {
            this.connection.close();
        }

        if (this.shardConnections != null) {
            for (ClickHouseConnection shardConnection : this.shardConnections) {
                shardConnection.close();
            }
        }
    }

    private String getJdbcUrl(String url, String database) throws SQLException {
        try {
            return "jdbc:" + (new URIBuilder(url)).setPath("/" + database).build().toString();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public String queryTableEngine(String databaseName, String tableName) throws SQLException {
        ClickHouseConnection conn = this.getConnection();
        try (PreparedStatement stmt = conn.prepareStatement("SELECT engine_full FROM system.tables WHERE database = ? AND name = ?")) {
            stmt.setString(1, databaseName);
            stmt.setString(2, tableName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("engine_full");
                }
            }
        }

        throw new SQLException("table `" + databaseName + "`.`" + tableName + "` does not exist");
    }
}
