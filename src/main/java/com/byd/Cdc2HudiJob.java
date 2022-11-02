package com.byd;

import com.byd.schema.TableRowData;
import com.byd.utils.CDCUtils;
import com.byd.utils.CheckpointUtils;
import com.byd.utils.HudiCatalogManager;
import com.byd.utils.HudiPipelineUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.util.HoodiePipeline;

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
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //checkpoint
        CheckpointUtils.setCheckpoint(env, "file:///d://ck");

        // db-cdc-source
        Configuration mysqlConf = new Configuration();
        mysqlConf.set(CDCUtils.HOST_NAME, "43.139.84.117");
        mysqlConf.set(CDCUtils.DATABASE, "test");
        mysqlConf.set(CDCUtils.TABLE_LIST_STR, "stu");
        MySqlSource<TableRowData> source = CDCUtils.createMysqlCDCSource(mysqlConf);

        // map
        SingleOutputStreamOperator<RowData> mysqlStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .map((MapFunction<TableRowData, RowData>) TableRowData::getRowData);

        // print
        mysqlStream.printToErr(">>>");

        // register catalog
        Configuration catalogConf = new Configuration();
        catalogConf.setString("catalog.path", "file:///d:/tmp/hoodie_catalog");
        HudiCatalogManager.registerHoodieCatalog(tenv, catalogConf);


        //hudi sink
        String tableName = "t1";
        String databaseName = "test";
        HoodiePipeline.Builder builder = HudiPipelineUtils.createHudiPipeline(tableName, databaseName);

        builder.sink(mysqlStream, false);
        env.execute("Api_Sink");
    }
}
