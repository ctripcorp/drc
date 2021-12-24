package com.ctrip.framework.drc.core.service.dal;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-28
 */
public enum DalClusterTypeEnum {

    DRC(0, "drc"),
    NORMAL(1, "normal");

    private String value;
    private Integer code;

    DalClusterTypeEnum(Integer code, String value) {
        this.code = code;
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public Integer getCode() {
        return code;
    }
}
