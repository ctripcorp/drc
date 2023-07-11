package com.ctrip.framework.drc.console.enums;

import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;

/**
 * Created by dengquanliang
 * 2023/7/11 14:26
 */
public enum FilterTypeEnum {
    ROWS_FILTER(0),
    COLUMNS_FILTER(1),
    MESSENGER_FILTER(2),
    ;

    private int code;

    FilterTypeEnum(int code) {
        this.code = code;
    }

    public static FilterTypeEnum getByCode (int code) {
        for (FilterTypeEnum value : FilterTypeEnum.values()) {
            if (code == value.code) {
                return value;
            }
        }
        throw ConsoleExceptionUtils.message(String.format("unexpected filterType： %s", code));
    }

    public int getCode() {
        return code;
    }

}
