package org.myorg.quickstart;

import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Kafka2Hive {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment stenv = StreamTableEnvironment.create(env, settings);


        // 设置检查点
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(20 * 1000L);
        checkpointConfig.setMinPauseBetweenCheckpoints(10 * 1000L);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setStateBackend(
                (StateBackend)
                        new FsStateBackend("hdfs:///flink/data-test-checkpoint", true)
        );
//
//        String myhive = "CREATE CATALOG myhive WITH (\n" +
//                "    'type' = 'hive',\n" +
//                "    'default-database' = 'tmp', \n"+
//                " 'hive-conf-dir' = '/Users/rf/xuefeng/gitrepo/github/flinkdemo/src/main/resources'\n"+
//                ")";
//        stenv.executeSql(myhive);

        String name            = "myhive";
        String defaultDatabase = "tmp";
        String hiveConfDir     = "src/main/resources";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        stenv.registerCatalog("myhive", hive);

// set the HiveCatalog as the current catalog of the session
        stenv.useCatalog("myhive");



        String kafkaTable = "CREATE TABLE if not exists tmp.user_log  (\n" +
                "  user_id STRING,\n" +
                "  item_id STRING,\n" +
                "  category_id STRING,\n" +
                "  behavior STRING,\n" +
                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +  //只使用 timestamp 的话报错
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_behavior',\n" +
                "  'properties.bootstrap.servers' = '192.168.1.105:9092',\n" +
                "  'properties.group.id' = 'flink-demo',\n" +
                "  'properties.value.deserializer' = 'org.apache.kafka.common.serialization.StringSerializer',\n" +
                "  'properties.key.deserializer' = 'org.apache.kafka.common.serialization.StringSerializer',\n" +
                "  'format' = 'json',\n" +
                "  'scan.startup.mode' = 'earliest-offset'\n" +
                ") ";
        stenv.executeSql(kafkaTable);
        stenv.executeSql("drop  TABLE if  exists tmp.pvuv_sink");


        stenv.getConfig().setSqlDialect(SqlDialect.HIVE);
        String hiveTable = "CREATE TABLE if not exists tmp.pvuv_sink (\n" +
                "    dt string,\n" +
                "    pv BIGINT,\n" +
                "    uv BIGINT\n" +
                ")  STORED AS parquet TBLPROPERTIES (\n" +
                //"  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n" +
                "  'sink.partition-commit.trigger'='partition-time',\n" +
                "  'sink.partition-commit.delay'='1 min',\n" +
                "  'sink.partition-commit.policy.kind'='metastore,success-file'\n" +
                ")";
        stenv.executeSql(hiveTable);

        String insertSelect = "INSERT INTO myhive.tmp.pvuv_sink\n" +
                "SELECT\n" +
                "  DATE_FORMAT(ts, 'yyyy-MM-dd HH:00') dt,\n" +
                "  100 AS pv,\n" +
                "  200 AS uv\n" +
                "FROM tmp.user_log\n" ;
        //hive table 不支持更新
                //"GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd HH:00')\n";
        stenv.executeSql(insertSelect).print();

        //stenv.sqlQuery("select * from  user_log_str").execute().print();


        //env.execute("Flink Streaming Java API Skeleton");
    }
}
