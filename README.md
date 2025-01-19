# seata-rm-async-worker-test

- For https://github.com/apache/shardingsphere/pull/34307 .

- Verified unit test under Ubuntu 22.04.4 LTS with `SDKMAN!` and `Docker CE`.

```shell
sdk install java 21.0.2-open

git clone git@github.com:linghengqian/seata-rm-async-worker-test.git
cd ./seata-rm-async-worker-test/
sdk use java 21.0.2-open
./mvnw -T 1C clean test
```

- The log is as follows.

```shell
$ ./mvnw -T 1C clean test
[INFO] Scanning for projects...
[INFO] 
[INFO] Using the MultiThreadedBuilder implementation with a thread count of 16
[INFO] 
[INFO] ---------< io.github.linghengqian:seata-rm-async-worker-test >----------
[INFO] Building seata-rm-async-worker-test 1.0-SNAPSHOT
[INFO]   from pom.xml
[INFO] --------------------------------[ jar ]---------------------------------
[INFO] 
[INFO] --- clean:3.2.0:clean (default-clean) @ seata-rm-async-worker-test ---
[INFO] Deleting /home/linghengqian/TwinklingLiftWorks/git/public/seata-rm-async-worker-test/target
[INFO] 
[INFO] --- resources:3.3.1:resources (default-resources) @ seata-rm-async-worker-test ---
[INFO] skip non existing resourceDirectory /home/linghengqian/TwinklingLiftWorks/git/public/seata-rm-async-worker-test/src/main/resources
[INFO] 
[INFO] --- compiler:3.13.0:compile (default-compile) @ seata-rm-async-worker-test ---
[INFO] No sources to compile
[INFO] 
[INFO] --- resources:3.3.1:testResources (default-testResources) @ seata-rm-async-worker-test ---
[INFO] Copying 4 resources from src/test/resources to target/test-classes
[INFO] 
[INFO] --- compiler:3.13.0:testCompile (default-testCompile) @ seata-rm-async-worker-test ---
[INFO] Recompiling the module because of changed source code.
[INFO] Compiling 1 source file with javac [debug target 21] to target/test-classes
[INFO] 
[INFO] --- surefire:3.2.5:test (default-test) @ seata-rm-async-worker-test ---
[INFO] Using auto detected provider org.apache.maven.surefire.junitplatform.JUnitPlatformProvider
[INFO] 
[INFO] -------------------------------------------------------
[INFO]  T E S T S
[INFO] -------------------------------------------------------
[INFO] Running io.github.linghengqian.SimpleTest
[ERROR] 2025-01-19 15:49:47.870 [main] o.a.s.config.ConfigurationFactory - failed to load non-spring configuration :not found service provider for : org.apache.seata.config.ConfigurationProvider
org.apache.seata.common.loader.EnhancedServiceNotFoundException: not found service provider for : org.apache.seata.config.ConfigurationProvider
[ERROR] 2025-01-19 15:50:56.225 [main] o.a.s.config.ConfigurationFactory - failed to load non-spring configuration :not found service provider for : org.apache.seata.config.ConfigurationProvider
org.apache.seata.common.loader.EnhancedServiceNotFoundException: not found service provider for : org.apache.seata.config.ConfigurationProvider
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 77.11 s -- in io.github.linghengqian.SimpleTest
[INFO] 
[INFO] Results:
[INFO] 
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
[INFO] 
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  01:20 min (Wall Clock)
[INFO] Finished at: 2025-01-19T15:50:57+08:00
[INFO] ------------------------------------------------------------------------
```