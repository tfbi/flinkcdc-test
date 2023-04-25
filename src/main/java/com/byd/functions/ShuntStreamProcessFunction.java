package com.byd.functions;

import com.byd.schema.RecordInfo;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;

public class ShuntStreamProcessFunction extends ProcessFunction<RecordInfo, RecordInfo> {

    private final Map<String, OutputTag<RecordInfo>> outputTagMap;
    private final String tagKeyFormat;

    public ShuntStreamProcessFunction(
            String tagKeyFormat, Map<String, OutputTag<RecordInfo>> outputTagMap) {
        this.outputTagMap = outputTagMap;
        this.tagKeyFormat = tagKeyFormat;
    }

    @Override
    public void processElement(
            RecordInfo recordInfo, Context context, Collector<RecordInfo> collector)
            throws Exception {
        String key = "";
        if (tagKeyFormat.equals("db.table")) {
            key = recordInfo.getSourceDb() + "." + recordInfo.getSourceTable();
        } else {
            key = recordInfo.getSourceSchema() + "." + recordInfo.getSourceTable();
        }
        if (outputTagMap.containsKey(key)) {
            context.output(outputTagMap.get(key), recordInfo);
        } else {
            collector.collect(recordInfo);
        }
    }
}
