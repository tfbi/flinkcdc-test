package com.byd.utils;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.hudi.util.HoodiePipeline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author bi.tengfei1
 */
public class HudiPipelineUtils {

    public static HoodiePipeline.Builder createHudiPipeline( String dbName,String tableName, Map<String, String> options) throws TableNotExistException {
        Catalog catalog = HudiCatalogManager.getCatalog();
        // get table
        CatalogTable table = (CatalogTable) catalog.getTable(new ObjectPath(dbName, tableName));

        HoodiePipeline.Builder builder = HoodiePipeline.builder(tableName);
        // set columns
        for (Schema.UnresolvedColumn column : table.getUnresolvedSchema().getColumns()) {
            String str = column.toString().replaceAll("NOT NULL", "");
            builder.column(str);
        }
        // set partition
        List<String> partitionKeys = table.getPartitionKeys();
        if (partitionKeys != null && partitionKeys.size() > 0) {
            String[] partitionArr = new String[partitionKeys.size()];
            builder.partition(partitionKeys.toArray(partitionArr));
        }
        //  set primaryKeys
        Optional<Schema.UnresolvedPrimaryKey> pk = table.getUnresolvedSchema().getPrimaryKey();
        if (pk.isPresent()) {
            Schema.UnresolvedPrimaryKey primaryKey = pk.get();
            List<String> columnNames = primaryKey.getColumnNames();
            String[] pkArr = new String[columnNames.size()];
            builder.pk(columnNames.toArray(pkArr));
        }

        //set options
        if (options != null && !options.isEmpty()) {
            builder.options(options);
        }
        if (table.getOptions() != null && !table.getOptions().isEmpty()) {
            builder.options(table.getOptions());
        }
        return builder;

    }

    public static HoodiePipeline.Builder createHudiPipeline(String tableName, String dbName) throws TableNotExistException {
        return createHudiPipeline(tableName, dbName, null);
    }

    public static Map<String, HoodiePipeline.Builder> checkTableAndCreatePipelineMap(String sourceDb, String sourceTbList, String hudiDb, String hudiTbList) throws Exception {
        Catalog catalog = HudiCatalogManager.getCatalog();
        String[] sourceTbArr = sourceTbList.split(",");
        String[] huidTbArr = hudiTbList.split(",");
        if (huidTbArr.length <= 0 || sourceTbArr.length != huidTbArr.length) {
            throw new Exception("source table is not match hudi table");
        }
        Map<String, HoodiePipeline.Builder> HudiPipelineMap = new HashMap<>();

        for (int i = 0; i < huidTbArr.length; i++) {
            String hudiTbName = huidTbArr[i];
            ObjectPath tablePath = new ObjectPath(hudiDb, hudiTbName);
            boolean flag = catalog.tableExists(tablePath);
            if (!flag) {
                throw new TableNotExistException("huid_catalog", tablePath);
            }
        }
        for (int i = 0; i < huidTbArr.length; i++) {
            String hudiTbName = huidTbArr[i];
            String sourceTbName = sourceTbArr[i];
            HoodiePipeline.Builder hudiPipeline = createHudiPipeline(hudiDb, hudiTbName, null);
            HudiPipelineMap.put(sourceDb + "." + sourceTbName, hudiPipeline);
        }
        return HudiPipelineMap;
    }
}
