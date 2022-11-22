package com.byd.schema;

import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import org.apache.flink.table.types.DataType;

public interface DeserializationConverter extends DeserializationRuntimeConverter {
    DataType getDataType();
}
