package com.ctrip.framework.drc.console.enums;

public enum SourceTypeEnum {
    INCREMENTAL(1, "incremental data consistency check"),
    FULL(2, "full data consistency check");
    private Integer code;
    private String desc;
    SourceTypeEnum(Integer code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public Integer getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
