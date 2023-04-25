package com.byd.utils;

import com.byd.schema.RecordInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
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

    public static void streamSinkToStarRocks(
            SingleOutputStreamOperator<RecordInfo> sourceStream,
            Map<String, OutputTag<RecordInfo>> outputMap,
            Map<String, SinkFunction<String>> sinkMap) {
        for (String key : outputMap.keySet()) {
            OutputTag<RecordInfo> outputTag = outputMap.get(key);
            SinkFunction<String> sink = sinkMap.get(key);
            DataStream<RecordInfo> outputStream = sourceStream.getSideOutput(outputTag);
            DataStream<String> stream =
                    outputStream.map(
                            (MapFunction<RecordInfo, String>)
                                    recordInfo -> recordInfo.getRow().toJsonWithKind());
            stream.printToErr(">>>");
            stream.addSink(sink);
        }
    }
}
