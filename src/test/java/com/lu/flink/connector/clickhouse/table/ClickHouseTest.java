package com.lu.flink.connector.clickhouse.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.row;

public class ClickHouseTest {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        Table table = tableEnvironment.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("dt", DataTypes.DATE()),
                        DataTypes.FIELD("order_id", DataTypes.INT()),
                        DataTypes.FIELD("movie_id", DataTypes.INT()),
                        DataTypes.FIELD("uid", DataTypes.INT())
                ),
                row("2021-09-01", 1, 1, 1),
                row("2021-09-01", 2, 2, 2),
                row("2021-09-01", 3, 3, 3)
        );
        tableEnvironment.createTemporaryView("source", table);
        tableEnvironment.executeSql(
                "CREATE TABLE sink (\n" +
                        "dt Date\n" +
                        ",order_id Int\n" +
                        ",movie_id Int\n" +
                        ",uid Int\n" +
                        ") WITH (" +
                        "'connector'='clickhouse'\n" +
                        ",'url'='clickhouse://172.21.0.4:8123'\n" +
                        ",'username'='default'\n" +
                        ",'password'=''\n" +
                        ",'database-name'='database_test'\n" +
                        ",'table-name'='order'\n" +
                        ",'sink.batch-size'='1'\n" +
                        ",'sink.write-local'='true'\n" +
                        ")"
        );
        tableEnvironment.executeSql(
                "INSERT INTO sink SELECT * FROM source"
        );
    }
}
