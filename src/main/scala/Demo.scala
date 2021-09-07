//import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
//
///**
// *
// */
//object Demo {
//
//  def main(args: Array[String]): Unit = {
//    // ----------------------------------------
//    // environment configuration
//    val settings = EnvironmentSettings
//      .newInstance()
//      .inStreamingMode()
//      //.inBatchMode()
//      .build()
//    val stenv = TableEnvironment.create(settings)
//    //--------------------------------------------
//
//
//    val stripMargin =
//      """CREATE TABLE user_log_str  (
//        |  user_id STRING,
//        |  item_id STRING,
//        |  category_id STRING,
//        |  behavior STRING,
//        |  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'
//        |) WITH (
//        |  'connector' = 'kafka',
//        |  'topic' = 'user_behavior',
//        |  'properties.bootstrap.servers' = '192.168.1.105:9092',
//        |  'properties.group.id' = 'flink-demo',
//        |  'properties.value.deserializer' = 'org.apache.kafka.common.serialization.StringSerializer',
//        |  'properties.key.deserializer' = 'org.apache.kafka.common.serialization.StringSerializer',
//        |  'format' = 'json',
//        |  'scan.startup.mode' = 'earliest-offset'
//        |) """.stripMargin
//
//
//
//    stenv.executeSql(stripMargin)
//
//
//   stenv.sqlQuery("select * from user_log ").execute().print()
//
//
//
//    val mysqlSink = """CREATE TABLE pvuv_sink (
//                      |    dt VARCHAR,
//                      |    pv BIGINT,
//                      |    uv BIGINT
//                      |) WITH (
//                      |    'connector.type' = 'jdbc', -- 使用 jdbc connector
//                      |    'connector.url' = 'jdbc:mysql://192.168.16.12:30306/flinkdemo', -- jdbc url
//                      |    'connector.table' = 'pvuv_sink', -- 表名
//                      |    'connector.username' = 'flinkdemo', -- 用户名
//                      |    'connector.password' = 'flinkdemo', -- 密码
//                      |    'connector.write.flush.max-rows' = '1' -- 默认5000条，为了演示改为1条
//                      |)""".stripMargin
//    stenv.executeSql(mysqlSink);
//
//
//    val insertSelect = """INSERT INTO pvuv_sink
//                         |SELECT
//                         |  DATE_FORMAT(ts, 'yyyy-MM-dd HH:00') dt,
//                         |  COUNT(*) AS pv,
//                         |  COUNT(DISTINCT user_id) AS uv
//                         |FROM user_log
//                         |GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd HH:00')
//                         |""".stripMargin
//    stenv.executeSql(insertSelect).print()
//
//
//  }
//
//}
