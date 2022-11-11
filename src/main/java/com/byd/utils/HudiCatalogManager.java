package com.byd.utils;

import static org.apache.hudi.table.catalog.CatalogOptions.*;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.hudi.table.catalog.HoodieCatalog;
import org.apache.hudi.table.catalog.HoodieHiveCatalog;


/**
 * @author bi.tengfei1
 */
public class HudiCatalogManager {

    private static HoodieCatalog hudiCatalog = null;
    private static HoodieHiveCatalog hudiHiveCatalog = null;
    private static int modeFlag = 0;

    public static synchronized void registerHoodieCatalog(TableEnvironment tenv, Configuration config) {
        if (hudiCatalog == null) {
            String path = config.get(CATALOG_PATH);
            if (path == null) {
                throw new CatalogException("missing necessary parameter");
            }
            hudiCatalog = new HoodieCatalog("hudi_catalog", config);
            tenv.registerCatalog("hudi_catalog", hudiCatalog);
            modeFlag = 1;
        } else {
            throw new CatalogException("hudi_catalog is already registered and you can use it directly...");
        }
    }

    public static synchronized void registerHoodieHiveCatalog(TableEnvironment tenv, Configuration config) {
        if (hudiHiveCatalog == null) {
            String hiveConfDir = config.get(HIVE_CONF_DIR);
            if (hiveConfDir == null) {
                throw new CatalogException("missing necessary parameters");
            }
            hudiHiveCatalog = new HoodieHiveCatalog("hudi_hive_catalog", config);
            tenv.registerCatalog("hudi_hive_catalog", hudiHiveCatalog);
            modeFlag = 2;
        } else {
            throw new CatalogException("hudi_hive_catalog is already registered and you can use it directly...");
        }
    }

    public static Catalog getCatalog() {
        if (modeFlag == 0) {
            throw new CatalogException("catalog has not been registered, please register first");
        } else if (modeFlag == 1) {
            return hudiCatalog;
        } else {
            return hudiHiveCatalog;
        }
    }
}
