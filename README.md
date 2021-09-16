# flinkdemo

>用来保存测试 演示 flink 的代码

```shell
wget https://dlcdn.apache.org/flink/flink-1.13.2/flink-1.13.2-bin-scala_2.11.tgz
```
### 运行 flinkdemo
```sh
bin/flink run  -m -m jobmanager_ip:port  flinkdemo/target/flinkdemo-0.1.jar
```



### 测试 wordcount
```  sh
bin/flink run  -m jobmanager_ip:port  \
./examples/batch/WordCount.jar \
--input hdfs:///tmp/people.txt \
--output hdfs:///tmp/flink_people_k8s
```

### 测试 flinkSQL

怎运行 flinkSQL需要提交端也
本地也需要有:
``` sh
wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.13.2/flink-sql-connector-kafka_2.11-1.13.2.jar

wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-hive_2.11/1.13.2/flink-connector-hive_2.11-1.13.2.jar

wget  https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-1.2.2_2.11/1.13.2/flink-sql-connector-hive-1.2.2_2.11-1.13.2.jar

wget  https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.6.5-10.0/flink-shaded-hadoop-2-uber-2.6.5-10.0.jar
```


#### 交互式
`flink-1.13.2> bin/start-scala-shell.sh  remote jobmanager_ip port`

```scala

scala> val stenv  = StreamTableEnvironment.create(senv)
scala> stenv.executeSql("""CREATE TABLE source_table ( user_id INT, cost DOUBLE ) WITH ( 'connector' = 'datagen', 'rows-per-second'='500', 'fields.user_id.kind'='random', 'fields.user_id.min'='1', 'fields.user_id.max'='10', 'fields.cost.kind'='random', 'fields.cost.min'='1', 'fields.cost.max'='100' )""");
scala> stenv.sqlQuery("select * from source_table").execute().print();
```
