package com.byd.schema;

import org.apache.flink.connectors.kudu.connector.writer.KuduOperationMapper;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class TRowOperationMapper implements KuduOperationMapper<TRow> {

    public Optional<Operation> createBaseOperation(TRow input, KuduTable table) {
        Optional<Operation> operation = Optional.empty();
        switch (input.getKind()) {
            case INSERT:
            case UPDATE_AFTER:
                operation = Optional.of(table.newUpsert());
                break;
            case DELETE:
                operation = Optional.of(table.newDelete());
        }
        return operation;
    }

    @Override
    public List<Operation> createOperations(TRow input, KuduTable table) {
        Optional<Operation> operationOpt = this.createBaseOperation(input, table);
        if (!operationOpt.isPresent()) {
            return Collections.emptyList();
        } else {
            Operation operation = (Operation) operationOpt.get();
            PartialRow partialRow = operation.getRow();

            for (int i = 0; i < input.getArity(); ++i) {
                partialRow.addObject(input.getFieldName(i), input.getField(i));
            }

            return Collections.singletonList(operation);
        }
    }
}
