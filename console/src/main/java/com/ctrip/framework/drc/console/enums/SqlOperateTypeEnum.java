package com.ctrip.framework.drc.console.enums;

/**
 * Created by dengquanliang
 * 2023/10/18 15:05
 */
public enum SqlOperateTypeEnum {
    UPDATE("UPDATE"),
    DELETE("DELETE"),
    INSERT("INSERT")
    ;

    private String name;

    SqlOperateTypeEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
