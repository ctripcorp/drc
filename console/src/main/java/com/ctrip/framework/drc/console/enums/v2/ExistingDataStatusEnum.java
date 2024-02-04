package com.ctrip.framework.drc.console.enums.v2;

/**
 * Created by dengquanliang
 * 2024/1/23 16:35
 */
public enum ExistingDataStatusEnum {
    NOT_PROCESSED(0),
    PROCESSING(1),
    PROCESSING_COMPLETED(2),
    NO_NEED_TO_PROCESSING(3),
    ;

    private int code;

    ExistingDataStatusEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
