package io.github.linghengqian;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.seata.config.ConfigurationFactory;
import org.apache.seata.core.rpc.netty.RmNettyRemotingClient;
import org.apache.seata.core.rpc.netty.TmNettyRemotingClient;
import org.apache.seata.rm.RMClient;
import org.apache.seata.rm.datasource.DataSourceProxy;
import org.apache.seata.tm.TMClient;
import org.awaitility.Awaitility;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@SuppressWarnings("SqlNoDataSourceInspection")
public class SimpleTest {

    @Test
    void test() {
        assertThat(System.getProperty("service.default.grouplist"), is(nullValue()));
        try (GenericContainer<?> seataContainer = new GenericContainer<>("apache/seata-server:2.2.0")) {
            seataContainer.withExposedPorts(7091, 8091).waitingFor(
                    Wait.forHttp("/health").forPort(7091).forStatusCode(200).forResponsePredicate("ok"::equals)
            );
            seataContainer.start();
            System.setProperty("service.default.grouplist", "127.0.0.1:" + seataContainer.getMappedPort(8091));
            TMClient.init("test-first", "default_tx_group");
            RMClient.init("test-first", "default_tx_group");
            Stream.of("demo_ds_0", "demo_ds_1", "demo_ds_2").parallel()
                    .forEach(databaseName -> {
                        try {
                            exSQL(databaseName);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });
            TmNettyRemotingClient.getInstance().destroy();
            RmNettyRemotingClient.getInstance().destroy();
            ConfigurationFactory.reload();
            System.clearProperty("service.default.grouplist");
        }
    }


    private void exSQL(String databaseName) throws SQLException {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("org.testcontainers.jdbc.ContainerDatabaseDriver");
        config.setJdbcUrl("jdbc:tc:postgresql:17.1-bookworm://test/%s?TC_INITSCRIPT=init.sql".formatted(databaseName));
        DataSource hikariDataSource = new HikariDataSource(config);
        DataSource seataDataSource = new DataSourceProxy(hikariDataSource);
        Awaitility.await().atMost(Duration.ofSeconds(15L)).ignoreExceptions().until(() -> {
            seataDataSource.getConnection().close();
            return true;
        });
        try (Connection connection = seataDataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS t_order (
                        order_id BIGSERIAL NOT NULL PRIMARY KEY,
                        order_type INTEGER,
                        user_id INTEGER NOT NULL,
                        address_id BIGINT NOT NULL,
                        status VARCHAR(50)
                    )
                    """);
            statement.execute("TRUNCATE TABLE t_order");
            IntStream.range(1, 11).forEach(i -> {
                try {
                    statement.executeUpdate("INSERT INTO t_order (order_id, user_id, order_type, address_id, status) VALUES (%d, %d, %d, %d, 'INSERT_TEST')".formatted(i, i, i % 2, i));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            statement.executeQuery("SELECT * FROM t_order");
            IntStream.range(1, 11).forEach(i -> {
                try {
                    statement.executeUpdate("DELETE FROM t_order WHERE order_id=%d".formatted(i));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            statement.executeQuery("SELECT * FROM t_order");
        }
        try (Connection connection = seataDataSource.getConnection()) {
            try {
                connection.setAutoCommit(false);
                connection.createStatement().executeUpdate("INSERT INTO t_order (order_id, user_id, order_type, address_id, status) VALUES (2024, 2024, 0, 2024, 'INSERT_TEST')");
                connection.createStatement().executeUpdate("INSERT INTO t_order_does_not_exist (test_id_does_not_exist) VALUES (2024)");
                connection.commit();
            } catch (final SQLException ignored) {
                connection.rollback();
            } finally {
                connection.setAutoCommit(true);
            }
        }
        try (Connection conn = seataDataSource.getConnection();
             ResultSet resultSet = conn.createStatement().executeQuery("SELECT * FROM t_order WHERE order_id = 2024")) {
            assertThat(resultSet.next(), CoreMatchers.is(false));
        }
        try (Connection connection = seataDataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS t_order");
        }
    }
}