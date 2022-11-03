package com.byd.utils;

import com.byd.schema.TableRowData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;

public class RowDataAndShuntProcessFunction extends ProcessFunction<TableRowData, RowData> {

    private final Map<String, OutputTag<RowData>> outputTagMap;

    public RowDataAndShuntProcessFunction(Map<String, OutputTag<RowData>> outputTagMap) {
        this.outputTagMap = outputTagMap;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void processElement(TableRowData tableRowData, Context ctx, Collector<RowData> out) throws Exception {
        String dbName = tableRowData.getSourceDb();
        String tableName = tableRowData.getSourceTable();
        RowData rowData = tableRowData.getRowData();
        if (outputTagMap.containsKey(dbName + "." + tableName)) {
            ctx.output(outputTagMap.get(dbName + "." + tableName), rowData);
        } else {
            out.collect(rowData);
        }
    }
}
