/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.byd.schema;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.*;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;


/**
 * Deserialization schema from Debezium object to Flink Table/SQL internal data structure {@link
 * RowData}.
 */
public final class TableRowDataDebeziumDeserializationSchema
        implements DebeziumDeserializationSchema<TableRowData> {

    private static final String SOURCE = "source";
    private static final String TABLE = "table";

    private final BaseSchemaConverter converter;

    public TableRowDataDebeziumDeserializationSchema(BaseSchemaConverter converter) {
        this.converter = converter;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<TableRowData> out) {
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        // get source table
        final Struct source = value.getStruct(SOURCE);
        String table = source.getString(TABLE);
        // judge rowKind
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            GenericRowData insert = extractAfterRow(value, valueSchema);
            insert.setRowKind(RowKind.INSERT);
            emit(insert, table, out);
        } else if (op == Envelope.Operation.DELETE) {
            GenericRowData delete = extractBeforeRow(value, valueSchema);
            delete.setRowKind(RowKind.DELETE);
            emit(delete, table, out);
        } else {
            GenericRowData before = extractBeforeRow(value, valueSchema);
            before.setRowKind(RowKind.UPDATE_BEFORE);
            emit(before, table, out);

            GenericRowData after = extractAfterRow(value, valueSchema);
            after.setRowKind(RowKind.UPDATE_AFTER);
            emit(after, table, out);
        }
    }

    private GenericRowData extractAfterRow(Struct value, Schema valueSchema) {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        return extractRow(after, valueSchema);
    }

    public GenericRowData extractRow(Struct value, Schema valueSchema) {
        List<Field> fields = value.schema().fields();
        GenericRowData rowData = new GenericRowData(fields.size() + 1);
        int pos = 0;
        for (Field field : fields) {
            String schemaName = field.schema().name();
            Object fieldValue;
            if (schemaName != null) {
                try {
                    fieldValue = this.converter.convert(schemaName, value.get(field), field.schema());
                } catch (Exception e) {
                    e.printStackTrace();
                    fieldValue = value.get(field);
                }
            } else {
                try {
                    Schema.Type type = field.schema().type();
                    fieldValue = this.converter.convert(type, value.get(field), field.schema());
                } catch (Exception e) {
                    e.printStackTrace();
                    fieldValue = value.get(field);
                }
            }
            rowData.setField(pos++, fieldValue);
        }
        // partition set
        try {
            rowData.setField(pos, converter.convert(Schema.Type.STRING, "part1", null));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rowData;
    }

    private GenericRowData extractBeforeRow(Struct value, Schema valueSchema) {
        Struct before = value.getStruct(Envelope.FieldName.BEFORE);
        return extractRow(before, valueSchema);
    }

    private void emit(RowData rowData, String sourceTable, Collector<TableRowData> collector) {
        collector.collect(TableRowData.builder().rowData(rowData).sourceTable(sourceTable).build());
    }

    @Override
    public TypeInformation<TableRowData> getProducedType() {
        return BasicTypeInfo.of(TableRowData.class);
    }

}
