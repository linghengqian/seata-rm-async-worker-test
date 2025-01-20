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
import org.firebirdsql.management.FBManager;
import org.firebirdsql.management.PageSizeConstants;
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
        CompletableFuture<Void> futureSecond = futureFirst.thenAccept(_ -> {
            CompletableFuture<Void> childFuture = CompletableFuture.runAsync(this::testWithSeataClient);
            Awaitility.await().atMost(5L, TimeUnit.MINUTES).until(childFuture::isDone);
        });
        futureSecond.thenAccept(_ -> {
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
        config.setJdbcUrl("jdbc:tc:postgresql:17.2-bookworm://test/demo_ds_0?TC_INITSCRIPT=init.sql");
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
        GenericContainer<?> firebirdContainer = new GenericContainer<>("ghcr.io/fdcastel/firebird:5.0.1");
        firebirdContainer.withEnv("FIREBIRD_ROOT_PASSWORD", "masterkey")
                .withEnv("FIREBIRD_USER", "alice")
                .withEnv("FIREBIRD_PASSWORD", "masterkey")
                .withEnv("FIREBIRD_DATABASE", "mirror.fdb")
                .withEnv("FIREBIRD_DATABASE_DEFAULT_CHARSET", "UTF8")
                .withExposedPorts(3050);
        firebirdContainer.start();
        try (FBManager fbManager = new FBManager()) {
            fbManager.setServer("localhost");
            fbManager.setUserName("alice");
            fbManager.setPassword("masterkey");
            fbManager.setFileName("/var/lib/firebird/data/mirror.fdb");
            fbManager.setPageSize(PageSizeConstants.SIZE_16K);
            fbManager.setDefaultCharacterSet("UTF8");
            fbManager.setPort(firebirdContainer.getMappedPort(3050));
            fbManager.start();
            fbManager.createDatabase("/var/lib/firebird/data/demo_ds_1.fdb", "alice", "masterkey");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("org.firebirdsql.jdbc.FBDriver");
        config.setJdbcUrl("jdbc:firebird://localhost:" + firebirdContainer.getMappedPort(3050) + "//var/lib/firebird/data/" + "demo_ds_1");
        config.setUsername("alice");
        config.setPassword("masterkey");
        DataSource hikariDataSource = new HikariDataSource(config);
        Awaitility.await().atMost(Duration.ofSeconds(15L)).ignoreExceptions().until(() -> {
            hikariDataSource.getConnection().close();
            return true;
        });
        firebirdContainer.close();
    }
}