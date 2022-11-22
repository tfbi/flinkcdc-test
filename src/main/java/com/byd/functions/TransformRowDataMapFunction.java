package com.byd.functions;

import com.byd.schema.RecordInfo;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.table.data.RowData;

public class TransformRowDataMapFunction extends RichMapFunction<RecordInfo, RowData> {
    @Override
    public RowData map(RecordInfo recordInfo) throws Exception {
        return recordInfo.getRow().toRowData();
    }
}
