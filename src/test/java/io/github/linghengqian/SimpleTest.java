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
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SimpleTest {

    @Test
    void test() {
        assertThat(System.getProperty("service.default.grouplist"), is(nullValue()));
        CompletableFuture<Void> futureFirst = CompletableFuture.runAsync(this::testWithSeataClient);
        CompletableFuture<Void> futureSecond = futureFirst.thenAccept(result -> {
            CompletableFuture<Void> childFuture = CompletableFuture.runAsync(this::testWithSeataClient);
            Awaitility.await().atMost(5L, TimeUnit.MINUTES).until(childFuture::isDone);
        });
        futureSecond.thenAccept(result -> {
            CompletableFuture<Void> childFuture = CompletableFuture.runAsync(this::testWithoutSeataClient);
            Awaitility.await().atMost(5L, TimeUnit.MINUTES).until(childFuture::isDone);
        });
        Awaitility.await().atMost(5L, TimeUnit.MINUTES).until(futureFirst::isDone);
        Awaitility.await().atMost(5L, TimeUnit.MINUTES).until(futureSecond::isDone);
    }

    private void testWithSeataClient() {
        GenericContainer<?> seataContainer = new GenericContainer<>("apache/seata-server:2.2.0");
        seataContainer.withExposedPorts(7091, 8091).waitingFor(
                Wait.forHttp("/health").forPort(7091).forStatusCode(200).forResponsePredicate("ok"::equals)
        );
        seataContainer.start();
        System.setProperty("service.default.grouplist", "127.0.0.1:" + seataContainer.getMappedPort(8091));
        TMClient.init("test-first", "default_tx_group");
        RMClient.init("test-first", "default_tx_group");
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("org.testcontainers.jdbc.ContainerDatabaseDriver");
        config.setJdbcUrl("jdbc:tc:postgresql:17.2-bookworm://test/%s?TC_INITSCRIPT=init.sql".formatted("demo_ds_0"));
        DataSource hikariDataSource = new HikariDataSource(config);
        DataSource seataDataSource = new DataSourceProxy(hikariDataSource);
        Awaitility.await().atMost(Duration.ofSeconds(15L)).ignoreExceptions().until(() -> {
            seataDataSource.getConnection().close();
            return true;
        });
        TmNettyRemotingClient.getInstance().destroy();
        RmNettyRemotingClient.getInstance().destroy();
        ConfigurationFactory.reload();
        System.clearProperty("service.default.grouplist");
        seataContainer.close();
    }

    private void testWithoutSeataClient() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("org.testcontainers.jdbc.ContainerDatabaseDriver");
        config.setJdbcUrl("jdbc:tc:postgresql:17.2-bookworm://test/%s?TC_INITSCRIPT=init.sql".formatted("demo_ds_1"));
        HikariDataSource hikariDataSource = new HikariDataSource(config);
        Awaitility.await().atMost(Duration.ofSeconds(15L)).ignoreExceptions().until(() -> {
            hikariDataSource.getConnection().close();
            return true;
        });
        hikariDataSource.close();
    }
}