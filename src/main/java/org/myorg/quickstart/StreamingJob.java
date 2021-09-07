/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart;

import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author xf
 */
public class StreamingJob {

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


        String kafkaTable = "CREATE TABLE user_log  (\n" +
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


        String mysqlTable = "CREATE TABLE pvuv_sink (\n" +
                "    dt VARCHAR PRIMARY KEY NOT ENFORCED,\n" +
                "    pv BIGINT,\n" +
                "    uv BIGINT\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://192.168.16.12:30306/flinkdemo?characterEncoding=utf8&useSSL=false', \n" +
                "    'table-name' = 'pvuv_sink', \n" +
                "    'username' = 'flinkdemo', \n" +
                "    'password' = 'flinkdemo', \n" +
                "    'sink.buffer-flush.max-rows' = '1' \n" +
                ")";
        stenv.executeSql(mysqlTable);

        String insertSelect = "INSERT INTO pvuv_sink\n" +
                "SELECT\n" +
                "  DATE_FORMAT(ts, 'yyyy-MM-dd HH:00') dt,\n" +
                "  COUNT(*) AS pv,\n" +
                "  COUNT(DISTINCT user_id) AS uv\n" +
                "FROM user_log\n" +
                "GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd HH:00')\n";
        stenv.executeSql(insertSelect).print();

        //stenv.sqlQuery("select * from  user_log_str").execute().print();


        //env.execute("Flink Streaming Java API Skeleton");
    }
}
