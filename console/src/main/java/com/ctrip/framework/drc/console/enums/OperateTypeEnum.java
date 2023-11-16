package com.ctrip.framework.drc.console.enums;

import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;

/**
 * Created by dengquanliang
 * 2023/11/14 14:17
 */
public enum OperateTypeEnum {
    INSERT(0),
    UPDATE(1),
    DELETE(2),
    ;

    private int code;
    OperateTypeEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static OperateTypeEnum getByOperateType(int code) {
        for (OperateTypeEnum value : OperateTypeEnum.values()) {
            if (value.code == code) {
                return value;
            }
        }
        throw ConsoleExceptionUtils.message("unsupported operateType: " + code);
    }
}
