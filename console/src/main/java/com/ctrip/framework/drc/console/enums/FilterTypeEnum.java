package com.ctrip.framework.drc.console.enums;

/**
 * Created by dengquanliang
 * 2023/4/26 16:24
 */
public enum FilterTypeEnum {
    BLACKLIST(1, "黑名单"),
    WHITELIST(2, "白名单"),
    ;

    private int code;
    private String desc;

    FilterTypeEnum(int code, String desc){
        this.code = code;
        this.desc = desc;
    }

    public static String getDescByCode(int code) {
        for (FilterTypeEnum value : FilterTypeEnum.values()) {
            if (code == value.code) {
                return value.desc;
            }
        }
        throw new IllegalArgumentException("Unexpected FilterType " + code);
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
