package com.ctrip.framework.drc.console.enums;

public enum MigrationStatusEnum {
    INIT("Init"),
    EX_STARTING( "ExStarting"),
    STARTING("Starting"),
    READY_TO_SWITCH_DAL("ReadyToSwitchDal"),
    SUCCESS("Success"),
    FAIL("Fail"),
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
