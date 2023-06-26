package com.ctrip.framework.drc.applier.activity.monitor;

public enum ConflictType {
    Commit(1),
    Rollback(0);
    
    private int val;

    public int getVal() {
        return val;
    }

    ConflictType(int val) {
        this.val = val;
    }
}
