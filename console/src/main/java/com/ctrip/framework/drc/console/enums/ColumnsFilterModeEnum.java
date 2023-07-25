package com.ctrip.framework.drc.console.enums;

/**
 * Created by dengquanliang
 * 2023/5/26 17:12
 */
public enum ColumnsFilterModeEnum {
    EXCLUDE(0, "exclude"),
    INCLUDE(1, "include"),
    REGEX(2, "regex"),
    ;

    private Integer code;
    private String name;

    ColumnsFilterModeEnum(Integer code, String name) {
        this.code = code;
        this.name = name;
    }

    public static String getNameByCode (Integer code) {
        for (ColumnsFilterModeEnum value : ColumnsFilterModeEnum.values()) {
            if (value.code.equals(code)) {
                return value.name;
            }
        }
        throw new IllegalArgumentException(String.format("Unexpected ColumnsFilterMode: %s", code));
    }

    public static int getCodeByName (String name) {
        for (ColumnsFilterModeEnum value : ColumnsFilterModeEnum.values()) {
            if (value.name.equals(name)) {
                return value.code;
            }
        }
        throw new IllegalArgumentException(String.format("Unexpected ColumnsFilterMode: %s", name));
    }

    public Integer getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

}
