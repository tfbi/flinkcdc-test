package com.byd.utils;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.hudi.util.HoodiePipeline;

import java.util.Map;

public class StreamUtils {
    public static void StreamSink2HudiPipeline(SingleOutputStreamOperator<RowData> sourceStream,
                                               Map<String, OutputTag<RowData>> outputMap,
                                               Map<String, HoodiePipeline.Builder> pipelineMap) {
        for (String key : outputMap.keySet()) {
            OutputTag<RowData> outputTag = outputMap.get(key);
            HoodiePipeline.Builder builder = pipelineMap.get(key);
            DataStream<RowData> outputStream = sourceStream.getSideOutput(outputTag);
            outputStream.printToErr(">>>");
            builder.sink(outputStream, false);
        }
    }
}
