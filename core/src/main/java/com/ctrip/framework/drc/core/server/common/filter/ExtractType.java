package com.ctrip.framework.drc.core.server.common.filter;

/**
 * Created by jixinwang on 2022/12/30
 */
public enum ExtractType {

    ROW("row"),

    COLUMN("column");

    private String name;

    ExtractType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static ExtractType getExtractType(String name) {
        for (ExtractType extractType : values()) {
            if (extractType.getName().equalsIgnoreCase(name)) {
                return extractType;
            }
        }

        throw new IllegalArgumentException("extract type does not exist");
    }
}
