package com.byd.schema;


import lombok.Builder;
import lombok.Data;

import java.io.Serializable;


@Data
@Builder
public class RecordInfo implements Serializable {
    private String sourceTable;
    private String sourceSchema;
    private String sourceDb;
    private String ddl;
    private TRow row;
    private RecordType recordType;
}
