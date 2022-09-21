package com.ctrip.framework.drc.core.server.common.filter.row;

/**
 * @Author limingdong
 * @create 2022/9/19
 */
public enum UserFilterMode {

    Udl("udl", 1),

    Uid("uid", 2);

    private String name;

    private int priority;

    UserFilterMode(String name, int priority) {
        this.name = name;
        this.priority = priority;
    }

    public String getName() {
        return name;
    }

    public int getPriority() {
        return priority;
    }

    public static UserFilterMode getUserFilterMode(String name) {
        for (UserFilterMode filterMode : values()) {
            if (filterMode.getName().equalsIgnoreCase(name)) {
                return filterMode;
            }
        }

        throw new UnsupportedOperationException("not support for name " + name);
    }
}
