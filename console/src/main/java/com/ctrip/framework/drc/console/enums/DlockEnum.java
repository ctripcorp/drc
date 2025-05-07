package com.ctrip.framework.drc.console.enums;

/**
 * Created by shiruixin
 * 2025/4/23 19:47
 */
public enum DlockEnum {
    AUTOCONFIG("AUTOCONFIG","AutoConfig");

    private String lockName;
    private String operator;

    DlockEnum(String lockName, String operator){
        this.lockName = lockName;
        this.operator = operator;
    }

    public String getLockName() {
        return lockName;
    }

    public void setLockName(String lockName) {
        this.lockName = lockName;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }
}
