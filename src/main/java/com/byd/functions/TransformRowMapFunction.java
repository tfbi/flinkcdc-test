package com.byd.functions;

import com.byd.schema.RecordInfo;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.types.Row;

public class TransformRowMapFunction extends RichMapFunction<RecordInfo, Row> {
    @Override
    public Row map(RecordInfo recordInfo) throws Exception {
        return recordInfo.getRow().toRow();
    }
}
