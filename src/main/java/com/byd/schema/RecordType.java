package com.byd.schema;


public enum RecordType {
    CREATE_TABLE("create_table"),
    DROP_TABLE("drop_table"),
    RENAME_TABLE("rename_table"),
    ALERT_TABLE_ADD_COLUMN("alert_table_add_column"),
    ALERT_TABLE_RENAME_COLUMN("alert_table_rename_column"),
    ALERT_TABLE_RETYPE_COLUMN("alert_table_retype_column"),
    ALERT_TABLE_DROP_COLUMN("alert_table_drop_column"),
    UNKNOWN("unknown"),
    ROW_DATA("row_data");

    private String val;

    RecordType(String val) {
        this.val = val;
    }

    public String getVal() {
        return val;
    }

    private static final String RENAME = "RENAME";
    private static final String CREATE = "CREATE";
    private static final String DROP = "DROP";
    private static final String ALTER = "ALTER";
    private static final String CHANGE = "CHANGE";
    private static final String ADD = "ADD";


    public static RecordType getRecordTypeByDDL(String ddl) {
        String[] ddlSplit = ddl.toUpperCase().split("\\s+");
        switch (ddlSplit[0]) {
            case CREATE:
                return CREATE_TABLE;
            case DROP:
                return DROP_TABLE;
            case RENAME:
                return RENAME_TABLE;
            case ALTER:
                switch (ddlSplit[3]) {
                    case DROP:
                        return ALERT_TABLE_DROP_COLUMN;
                    case ADD:
                        return ALERT_TABLE_ADD_COLUMN;
                    case CHANGE:
                        return ddlSplit[4].equals(ddlSplit[5]) ? ALERT_TABLE_RETYPE_COLUMN : ALERT_TABLE_RENAME_COLUMN;
                    default:
                        return UNKNOWN;
                }
            default:
                return UNKNOWN;
        }
    }
}
