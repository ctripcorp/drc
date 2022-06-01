package com.ctrip.framework.drc.core.server.common.enums;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public enum RowsFilterType {

    None("none"),

    AviatorRegex("aviator_regex"),

    JavaRegex("java_regex"),

    TripUid("trip_uid"),

    Custom("custom");

    private String name;

    RowsFilterType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static RowsFilterType getType(String name) {
        for (RowsFilterType filterType : values()) {
            if (filterType.getName().equalsIgnoreCase(name)) {
                return filterType;
            }
        }

        throw new UnsupportedOperationException("not support for name " + name);
    }
}
