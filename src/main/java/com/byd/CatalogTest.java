package com.byd;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.hudi.table.catalog.HoodieCatalog;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

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
        tenv.executeSql("select * from t1").print();
    }
}
