package com.ctrip.framework.drc.fetcher.conflict.enums;

public enum ConflictResult {
    COMMIT(0),
    ROLLBACK(1);

    private int value;

    ConflictResult(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
