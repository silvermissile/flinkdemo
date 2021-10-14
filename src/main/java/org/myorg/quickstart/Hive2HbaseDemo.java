package org.myorg.quickstart;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Hive2HbaseDemo {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment stenv = StreamTableEnvironment.create(env, settings);




        String name            = "myhive";
        String defaultDatabase = "tmp";
        String hiveConfDir     = "src/main/resources";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        stenv.registerCatalog("myhive", hive);

// set the HiveCatalog as the current catalog of the session
        stenv.useCatalog("myhive");



        String hbaseTable = "CREATE TABLE if not exists tmp.h9Table (\n" +
                " dt string,\n" +
                " family1 ROW<pv  bigint>,\n" +
                " family2 ROW<uv  bigint>,\n" +
                " PRIMARY KEY (dt) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-1.4',\n" +
                " 'table-name' = 'h9Table',\n" +
                " 'zookeeper.quorum' = '192.168.1.76:2181,192.168.1.77:2181,192.168.1.78:2181'\n" +
                ")";
        stenv.executeSql(hbaseTable);

        String insertSelect = "INSERT INTO tmp.h9Table\n" +
                "  SELECT dt, ROW(pv), ROW(uv) FROM tmp.pvuv_sink" ;




//        System.out.println(hbaseTable);
//        System.out.println(insertSelect);

        //stenv.executeSql(insertSelect).print();


    }
}
