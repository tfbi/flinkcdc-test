package com.byd.functions;

import com.byd.schema.RecordInfo;
import org.apache.flink.api.java.functions.KeySelector;

public class KeyedByDBTableSelector implements KeySelector<RecordInfo, String> {
    @Override
    public String getKey(RecordInfo recordInfo) throws Exception {
        return recordInfo.getSourceDb() + "." + recordInfo.getSourceTable();
    }
}
