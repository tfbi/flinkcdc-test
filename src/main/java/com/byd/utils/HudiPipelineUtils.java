package com.byd.utils;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.hudi.util.HoodiePipeline;

import java.util.List;
import java.util.Optional;

/**
 * @author bi.tengfei1
 */
public class HudiPipelineUtils {

    public static HoodiePipeline.Builder createHudiPipeline(String tableName, String dbName) throws TableNotExistException {
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
        builder.options(table.getOptions());
        return builder;
    }
}
