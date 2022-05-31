package com.ctrip.framework.drc.core.server.config.applier.dto;


/**
 * Created by jixinwang on 2021/9/14
 */
public enum ApplyMode {

    set_gtid(0, "set_gtid"),
    transaction_table(1, "transaction_table");

    public static ApplyMode getApplyMode(final int type) {
        for(ApplyMode applyMode : values()) {
            if (applyMode.type == type) {
                return applyMode;
            }
        }
        return set_gtid;
    }

    ApplyMode(int type, String name) {
        this.type = type;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    private int type;
    private String name;
}
