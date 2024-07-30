package com.ctrip.framework.drc.console.enums;

import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;

/**
 * Created by dengquanliang
 * 2023/6/25 16:36
 */
public enum RowsFilterModeEnum {
    JAVA_REGEX(0, "java_regex"),
    TRIP_UDL(1, "trip_udl"),
    TRIP_UID(2, "trip_uid"),
    AVIATOR_REGEX(3, "aviator_regex"),
    CUSTOM(4, "custom"),
    TRIP_UDL_UID(5,"trip_udl_uid"),
    CUSTOM_SOA(6,"custom_soa"),
    ;

    private Integer code;
    private String name;

    RowsFilterModeEnum(Integer code, String name) {
        this.code = code;
        this.name = name;
    }

    public static String getNameByCode (Integer code) {
        for (RowsFilterModeEnum value : RowsFilterModeEnum.values()) {
            if (value.code.equals(code)) {
                return value.name;
            }
        }
        throw ConsoleExceptionUtils.message(String.format("Unexpected RowsFilterMode: %s", code));
    }

    public static int getCodeByName (String name) {
        for (RowsFilterModeEnum value : RowsFilterModeEnum.values()) {
            if (value.name.equals(name)) {
                return value.code;
            }
        }
        throw ConsoleExceptionUtils.message(String.format("Unexpected RowsFilterMode: %s", name));
    }

    public static boolean checkMode(int code) {
        for (RowsFilterModeEnum value : RowsFilterModeEnum.values()) {
            if (value.code == code) {
                return true;
            }
        }
        return false;
    }

    public Integer getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
