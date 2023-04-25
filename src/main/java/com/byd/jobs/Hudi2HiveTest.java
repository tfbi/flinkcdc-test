package com.byd.jobs;

import com.byd.utils.CheckpointUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import java.util.HashMap;

public class Hudi2HiveTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //set checkpoint
        CheckpointUtils.setCheckpoint(env, "hdfs://master02-cdpdev-ic:8020/tmp/flink_ck_2");

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("10.54.131.65", 9988);
        SingleOutputStreamOperator<RowData> stream = stringDataStreamSource.map(new MapFunction<String, RowData>() {
            @Override
            public RowData map(String s) throws Exception {
                String[] split = s.split(",");
                GenericRowData rowData = new GenericRowData(3);
                rowData.setField(0, Long.parseLong(split[0]));
                rowData.setField(1, StringData.fromString(split[1]));
                rowData.setField(2, Integer.parseInt(split[2]));
                return rowData;
            }
        });

        String tableName = "hudi_test1";
        String basePath = "hdfs://master02-cdpdev-ic:8020/tmp/hudi/" + tableName;
        HashMap<String, String> options = new HashMap<>();
        options.put(FlinkOptions.PATH.key(), basePath);
        options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
        options.put(FlinkOptions.HIVE_SYNC_ENABLED.key(), "true");
        options.put(FlinkOptions.HIVE_SYNC_MODE.key(), "hms");
        options.put(FlinkOptions.HIVE_SYNC_METASTORE_URIS.key(), "thrift://master02-cdpdev-ic:9083");
        options.put(FlinkOptions.HIVE_SYNC_DB.key(), "db_test");
        options.put(FlinkOptions.HIVE_SYNC_TABLE.key(), tableName);
        options.put(FlinkOptions.HIVE_SYNC_CONF_DIR.key(), "/etc/hive/conf");

        //options.put(FlinkOptions.HIVE_SYNC_PARTITION_FIELDS.key(), "pt");

        HoodiePipeline.Builder builder = HoodiePipeline.builder(tableName)
                .column("id BIGINT")
                .column("name STRING")
                .column("age INT")
                .pk("id")
                //.partition("pt")
                .options(options);

        builder.sink(stream, false);
        env.execute("FlinkHudiBundleDemoHms");
    }
}
