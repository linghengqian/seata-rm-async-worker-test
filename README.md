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

```