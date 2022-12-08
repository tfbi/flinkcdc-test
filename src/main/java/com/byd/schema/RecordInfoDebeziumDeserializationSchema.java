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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;


/**
 * Deserialization schema from Debezium object to Flink Table/SQL internal data structure {@link
 * RowData}.
 *
 * @author bi.tengfei1
 */
public final class RecordInfoDebeziumDeserializationSchema
        implements DebeziumDeserializationSchema<RecordInfo> {

    private static final String SOURCE = "source";
    private static final String TABLE = "table";
    private static final String DATABASE = "db";
    private static final String SCHEMA = "schema";
    private static final String HISTORY_RECORD = "historyRecord";
    private static final String DDL = "ddl";

    private final BaseSchemaConverter converter;

    public RecordInfoDebeziumDeserializationSchema(BaseSchemaConverter converter) {
        this.converter = converter;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<RecordInfo> out) {
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        // get source table
        final Struct source = value.getStruct(SOURCE);
        String table = source.getString(TABLE);
        String schema = source.getString(SCHEMA);
        String database = source.getString(DATABASE);
        if (op == null) {
            // schema change
            String historyRecord = value.getString(HISTORY_RECORD);
            JsonObject obj = new JsonParser().parse(historyRecord).getAsJsonObject();
            String ddl = obj.get(DDL).getAsString();
            emit(ddl, database, schema, table, RecordType.getRecordTypeByDDL(ddl), out);
        } else {
            // judge rowKind
            if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
                TRow insert = extractAfterRow(value, valueSchema);
                insert.setKind(RowKind.INSERT);
                emit(insert, database, schema, table, out);
            } else if (op == Envelope.Operation.DELETE) {
                TRow delete = extractBeforeRow(value, valueSchema);
                delete.setKind(RowKind.DELETE);
                emit(delete, database, schema, table, out);
            } else {
                TRow before = extractBeforeRow(value, valueSchema);
                before.setKind(RowKind.UPDATE_BEFORE);
                emit(before, database, schema, table, out);

                TRow after = extractAfterRow(value, valueSchema);
                after.setKind(RowKind.UPDATE_AFTER);
                emit(after, database, schema, table, out);
            }
        }
    }

    private TRow extractAfterRow(Struct value, Schema valueSchema) {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        return extractRow(after, valueSchema);
    }

    public TRow extractRow(Struct value, Schema valueSchema) {
        List<Field> fields = value.schema().fields();
        TRow row = new TRow(fields.size());
        int pos = 0;
        for (Field field : fields) {
            String schemaName = field.schema().name();
            String fileName = field.name();
            Object fieldValue = null;
            DataType fieldType = null;
            if (schemaName != null) {
                try {
                    fieldValue = this.converter.convert(schemaName, value.get(field), field.schema());
                    fieldType = this.converter.getDataType(schemaName);
                } catch (Exception e) {
                    fieldValue = value.get(field);
                }

            } else {
                try {
                    Schema.Type type = field.schema().type();
                    Object val = value.get(field);
                    fieldValue = this.converter.convert(type, val, field.schema());
                    fieldType = this.converter.getDataType(type);
                } catch (Exception e) {
                    fieldValue = value.get(field);
                }
            }
            row.setField(pos++, fileName, fieldValue, fieldType);
        }
        return row;
    }

    private TRow extractBeforeRow(Struct value, Schema valueSchema) {
        Struct before = value.getStruct(Envelope.FieldName.BEFORE);
        return extractRow(before, valueSchema);
    }

    private void emit(TRow row, String sourceDb, String sourceSchema, String sourceTable, Collector<RecordInfo> collector) {
        collector.collect(RecordInfo.builder().recordType(RecordType.ROW_DATA).row(row).sourceDb(sourceDb).sourceSchema(sourceSchema).sourceTable(sourceTable).build());
    }

    private void emit(String ddl, String sourceDb, String sourceTable, String sourceSchema, RecordType recordType, Collector<RecordInfo> collector) {
        collector.collect(RecordInfo.builder().recordType(recordType).ddl(ddl).sourceDb(sourceDb).sourceSchema(sourceSchema).sourceTable(sourceTable).build());
    }

    @Override
    public TypeInformation<RecordInfo> getProducedType() {
        return BasicTypeInfo.of(RecordInfo.class);
    }
}
