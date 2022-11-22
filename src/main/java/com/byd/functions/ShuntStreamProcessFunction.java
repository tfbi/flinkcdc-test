package com.byd.functions;

import com.byd.schema.RecordInfo;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.util.Map;

public class ShuntStreamProcessFunction extends KeyedProcessFunction<String, RecordInfo, RowData> {

    private final Map<String, OutputTag<RowData>> outputTagMap;

    public ShuntStreamProcessFunction(Map<String, OutputTag<RowData>> outputTagMap) {
        this.outputTagMap = outputTagMap;
    }

    @Override
    public void processElement(RecordInfo recordInfo, Context context, Collector<RowData> collector) throws Exception {
        String key = context.getCurrentKey();
        RowData t = recordInfo.getRow().toRowDataWithPartition(StringData.fromString("default"));
        if (outputTagMap.containsKey(key)) {
            context.output(outputTagMap.get(key), t);
        } else {
            collector.collect(t);
        }
    }
}
