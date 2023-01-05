package com.byd.functions;

import com.byd.schema.RecordInfo;
import com.byd.schema.RecordType;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class
FilterAndAlarmRecordProcessFunction extends ProcessFunction<RecordInfo, RecordInfo> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }


    @Override
    public void processElement(RecordInfo recordInfo, Context context, Collector<RecordInfo> collector) throws Exception {
        if (recordInfo.getRecordType() != RecordType.ROW_DATA) {
            alarm(recordInfo);
        } else {
            collector.collect(recordInfo);
        }
    }

    public void alarm(RecordInfo recordInfo) {
        System.out.println("系统警告： Schema change" + recordInfo);
    }
}
