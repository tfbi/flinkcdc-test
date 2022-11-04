package com.byd;

import com.byd.schema.TableRowData;
import com.byd.utils.*;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.hudi.util.HoodiePipeline;

import java.util.Map;

public class Cdc2HudiJob {
    public static void main(String[] args) throws Exception {
        try {
            runAPP();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runAPP() throws Exception {
        // flink env tenv
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //set checkpoint
        CheckpointUtils.setCheckpoint(env, "hdfs://master02-cdpdev-ic:8020/tmp/flink_ck");

        String sourceDb = "test";
        String sourceTbList = "stu,person";
        String hudiDb = "test";
        String hudiTbList = "t1,t2";

        // register hudi catalog
        Configuration catalogConf = new Configuration();
        catalogConf.setString("catalog.path", "hdfs://master02-cdpdev-ic:8020/tmp/hudi_catalog");
        HudiCatalogManager.registerHoodieCatalog(tenv, catalogConf);

        // check table and create pipeline
        Map<String, HoodiePipeline.Builder> pipelineMap = HudiPipelineUtils.checkTableAndCreatePipelineMap(sourceDb, sourceTbList, hudiDb, hudiTbList);

        // db-cdc-source
        Configuration mysqlConf = new Configuration();
        mysqlConf.set(SourceUtils.HOST_NAME, "10.17.7.197");
        mysqlConf.set(SourceUtils.USERNAME, "canal");
        mysqlConf.set(SourceUtils.PASSWORD, "Bigdata@123");
        mysqlConf.set(SourceUtils.DATABASE, sourceDb);
        mysqlConf.set(SourceUtils.TABLE_LIST_STR, sourceTbList);
        MySqlSource<TableRowData> source = SourceUtils.createMysqlCDCSource(mysqlConf);
        Map<String, OutputTag<RowData>> outputMap = SourceUtils.createOutputMap(mysqlConf);

        // create cdcStream, map rowData and shunt stream
        SingleOutputStreamOperator<RowData> sourceStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Source")
                .process(new RowDataAndShuntProcessFunction(outputMap));

        //stream - sink
        StreamUtils.StreamSink2HudiPipeline(sourceStream, outputMap, pipelineMap);

        env.execute("flink-cdc_hudi-test");
    }
}
