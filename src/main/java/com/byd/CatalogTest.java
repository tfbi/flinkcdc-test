package com.byd;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.hudi.table.catalog.HoodieHiveCatalog;

public class CatalogTest {
    public static void main(String[] args) throws TableNotExistException {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tenv = TableEnvironment.create(settings);
        Configuration config = new Configuration();
        Configuration catalogConf = new Configuration();
        catalogConf.setString("catalog.path", "hdfs://master02-cdpdev-ic:8020/tmp/hudi_hive_catalog");
        catalogConf.setString("hive.conf.dir", "/etc/hive/conf");
        catalogConf.setString("default", "hudi_db");
        HoodieHiveCatalog hudi_catalog = new HoodieHiveCatalog("hudi_hive_catalog", config);
        tenv.registerCatalog("hudi_hive_catalog", hudi_catalog);
        tenv.useCatalog("hudi_hive_catalog");
        tenv.executeSql("use hudi_db");
        tenv.executeSql("CREATE TABLE t2(" +
                "  id BIGINT PRIMARY KEY NOT ENFORCED," +
                "  name STRING," +
                "  company STRING," +
                "  weight DECIMAL(16,2)," +
                "  `partition` STRING" +
                ")" +
                "PARTITIONED BY (`partition`)" +
                "WITH (" +
                "  'connector'='hudi'," +
                "  'path'='file:///d://hudi/t2'," +
                "  'table.type'='MERGE_ON_READ'" +
                ")");
        tenv.executeSql("select * from t2").print();
    }
}
