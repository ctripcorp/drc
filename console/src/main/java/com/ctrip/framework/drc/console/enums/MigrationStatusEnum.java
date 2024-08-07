package com.ctrip.framework.drc.console.enums;

public enum MigrationStatusEnum {
    INIT("Init"),
    PRE_STARTING( "PreStarting"),
    PRE_STARTED( "PreStarted"),
    STARTING("Starting"),
    READY_TO_SWITCH_DAL("ReadyToSwitchDal"),
    READY_TO_COMMIT_TASK("ReadyToCommitTask"),
    SUCCESS("Success"),
    FAIL("Fail"),
    CANCELED("Canceled"),
    ;
    private String status;
    
    MigrationStatusEnum(String status) {
        this.status = status;
    }

    public MigrationStatusEnum getByStatus(String status) {
        for (MigrationStatusEnum value : MigrationStatusEnum.values()) {
            if (value.status.equals(status)) {
                return value;
            }
        }
        throw new IllegalArgumentException(String.format("unknown MigrationStatusEnum: %s", status));
    }
    
    public String getStatus() {
        return status;
    }
}
