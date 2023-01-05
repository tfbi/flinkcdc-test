package com.byd;

import com.byd.functions.FilterAndAlarmRecordProcessFunction;
import com.byd.functions.KeyedByDBTableSelector;
import com.byd.functions.ShuntStreamProcessFunction;
import com.byd.schema.RecordInfo;
import com.byd.utils.*;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import java.util.HashMap;
import java.util.Map;

public class Cdc2HudiJob {
    public static void main(String[] args) {
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
        env.setParallelism(4);

        //set checkpoint
        CheckpointUtils.setCheckpoint(env, "hdfs://master02-cdpdev-ic:8020/tmp/flink_ck");

        String sourceDb = "test";
        String sourceTbList = "stu,person";
        String hudiDb = "db_test";
        String hudiTbList = "hudi_t1,hudi_t2";

        // register hudi catalog
        Configuration catalogConf = new Configuration();
        catalogConf.setString("hive.conf.dir", "/etc/hive/conf");
        catalogConf.setString("default", hudiDb);
        HudiCatalogManager.registerHoodieHiveCatalog(tenv, catalogConf);

        HashMap<String, String> options = new HashMap<>();
        options.put(FlinkOptions.HIVE_SYNC_CONF_DIR.key(), "/etc/hive/conf");
        options.put(FlinkOptions.HIVE_SYNC_DB.key(), "db_test");

        // check table and create pipeline
        Map<String, HoodiePipeline.Builder> pipelineMap = HudiPipelineUtils.checkTableAndCreatePipelineMap(sourceDb, sourceTbList, hudiDb, hudiTbList, options);

        // db-cdc-source
        Configuration mysqlConf = new Configuration();
        mysqlConf.set(SourceUtils.HOST_NAME, "10.17.7.197");
        mysqlConf.set(SourceUtils.USERNAME, "canal");
        mysqlConf.set(SourceUtils.PASSWORD, "Bigdata@123");
        mysqlConf.set(SourceUtils.DATABASE, sourceDb);
        mysqlConf.set(SourceUtils.TABLE_LIST_STR, sourceTbList);
        MySqlSource<RecordInfo> source = SourceUtils.createMysqlCDCSource(mysqlConf);

        // create cdcStream, map rowData and
        KeyedStream<RecordInfo, String> originStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Source")
                .process(new FilterAndAlarmRecordProcessFunction())
                .keyBy(new KeyedByDBTableSelector());


        DataStream<RecordInfo> source1 = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Source")
                .process(new FilterAndAlarmRecordProcessFunction());



        // shunt stream
        Map<String, OutputTag<RowData>> outputMap = SourceUtils.createOutputMap(mysqlConf);
        SingleOutputStreamOperator<RowData> sourceStream = originStream.process(new ShuntStreamProcessFunction(outputMap));
        //stream - sink
        StreamUtils.StreamSink2HudiPipeline(sourceStream, outputMap, pipelineMap);

        env.execute("flink-cdc_hudi-test");
    }
}
