package com.byd;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.hudi.table.catalog.HoodieCatalog;

public class CatalogTest {
    public static void main(String[] args) throws TableNotExistException {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tenv = TableEnvironment.create(settings);
        Configuration config = new Configuration();
        config.setString("catalog.path", "file:///d:/tmp/hoodie_catalog");
        config.setString("default-database", "default");
        config.setString("mode", "dfs");
        HoodieCatalog hudi_catalog = new HoodieCatalog("hudi_catalog", config);
        tenv.registerCatalog("hudi_catalog", hudi_catalog);
        tenv.useCatalog("hudi_catalog");
        tenv.executeSql("use test");
//        tenv.executeSql("drop table t2");
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
