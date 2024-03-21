package com.ctrip.framework.drc.console.enums.log;

public enum ConflictCountType {
    CONFLICT_COMMIT_TRX(false,0),
    CONFLICT_ROLLBACK_TRX(false,1),
    CONFLICT_COMMIT_ROW(true,2),
    CONFLICT_ROLLBACK_ROW(true,3),
    CONFLICT_ROW(true,4),
    ;
    
    private boolean isRowCount;
    private Integer code;

    ConflictCountType(boolean isRowCount, Integer code) {
        this.isRowCount = isRowCount;
        this.code = code;
    }
    
    public boolean isRollback() {
        return this == CONFLICT_ROLLBACK_TRX || this == CONFLICT_ROLLBACK_ROW;
    }

    public boolean isRowCount() {
        return isRowCount;
    }

    public Integer getCode() {
        return code;
    }
}
