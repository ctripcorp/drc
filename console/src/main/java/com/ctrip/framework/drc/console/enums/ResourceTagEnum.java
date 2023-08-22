package com.ctrip.framework.drc.console.enums;

/**
 * Created by dengquanliang
 * 2023/8/4 14:39
 */
public enum ResourceTagEnum {
    COMMON(0, "COMMON"),
    FLT(1, "FLT"),
    HTL(2, "HTL"),
    OI(3, "OI"),
    TEST(4, "TEST"),
    ;

    private int code;
    private String name;

    ResourceTagEnum(int code, String name) {
        this.code = code;
        this.name = name;
    }

    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
