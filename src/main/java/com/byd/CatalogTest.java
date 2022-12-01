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
        tenv.executeSql("CREATE TABLE t1(" +
                "  id BIGINT PRIMARY KEY NOT ENFORCED," +
                "  name STRING," +
                "  age INT," +
                "  `partition` STRING" +
                ")" +
                "PARTITIONED BY (`partition`)" +
                "WITH (" +
                "  'connector'='hudi'," +
                "  'path'='file:///d://hudi/t1'," +
                "  'table.type'='MERGE_ON_READ'" +
                ")");
        tenv.executeSql("select * from t1").print();
    }
}
