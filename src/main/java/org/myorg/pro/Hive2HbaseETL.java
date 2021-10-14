package org.myorg.pro;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Hive2HbaseETL {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment stenv = StreamTableEnvironment.create(env, settings);

        Configuration config = stenv.getConfig().getConfiguration();
        config.setInteger("sql-client.display.max-column-width",500);


        String name = "myhive";
        String defaultDatabase = "tmp";
        //在本地 ide 中运行
        //String hiveConfDir     = "src/main/resources";

        //在集群中运行
        String hiveConfDir = "/etc/hive/conf";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        stenv.registerCatalog("myhive", hive);

// set the HiveCatalog as the current catalog of the session
        stenv.useCatalog("myhive");


        String hbaseTable = "CREATE TABLE if not exists tmp.h9Table3 (\n" +
                " unique_id_md5 string,\n" +
                " family1 ROW<first_install_time bigint,unique_id_md5 string,unique_id string>,\n" +
                " PRIMARY KEY (unique_id_md5) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-1.4',\n" +
                " 'table-name' = 'app_fisrt_install_time3',\n" +
                " 'zookeeper.quorum' = '192.168.1.76:2181,192.168.1.77:2181,192.168.1.78:2181'\n" +
                ")";
        stenv.executeSql(hbaseTable);



        String insertSelect = "INSERT INTO tmp.h9Table3\n" +
                "  SELECT MD5(CONCAT_WS('',user_id,device_id,package_name)), ROW(first_install_time,unique_id_md5,unique_id) FROM tmp.guojx_fit_query4 ";

//        String explain = stenv.explainSql(insertSelect);
//        System.out.println(explain);

//        String queryHbase = "select * from  tmp.h9Table  limit 10 ";
//        stenv.sqlQuery(queryHbase).execute().print();

//        String queryHbase1 = "SELECT  MD5(CONCAT_WS('','2001179642','ffffffff-9816-379d-ffff-ffffcd8a3453','com.lbe.security.miui'))";
//        stenv.sqlQuery(queryHbase1).execute().print();
//
//
//        String queryHbase2= "SELECT CONCAT_WS('','2001179642','ffffffff-9816-379d-ffff-ffffcd8a3453','com.lbe.security.miui')";
//        stenv.sqlQuery(queryHbase2).execute().print();

        stenv.executeSql(insertSelect).print();


    }
}
