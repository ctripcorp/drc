package com.ctrip.framework.drc.core.server.common.filter.column;

/**
 * Created by jixinwang on 2022/12/29
 */
public enum ColumnsFilterMode {

    NONE("none"),

    EXCLUDE("exclude"),

    INCLUDE("include");

    private String name;

    ColumnsFilterMode(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static ColumnsFilterMode getColumnFilterMode(String name) {
        for (ColumnsFilterMode filterMode : values()) {
            if (filterMode.getName().equalsIgnoreCase(name)) {
                return filterMode;
            }
        }

        throw new IllegalArgumentException("column filter mode does not exist");
    }
}
