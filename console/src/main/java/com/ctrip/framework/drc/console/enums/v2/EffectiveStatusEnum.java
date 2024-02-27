package com.ctrip.framework.drc.console.enums.v2;

/**
 * Created by dengquanliang
 * 2024/2/21 17:04
 */
public enum EffectiveStatusEnum {
    NOT_IN_EFFECT(0),
    IN_EFFECT(1),
    EFFECTIVE(2),
    ;

    private int code;

    EffectiveStatusEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
