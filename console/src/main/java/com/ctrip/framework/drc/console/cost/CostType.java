package com.ctrip.framework.drc.console.cost;

/**
 * Created by jixinwang on 2022/9/6
 */
public enum CostType {

    storage(0, "storage"),
    flow(1, "flow");

    public static CostType getCostType(final int type) {
        for(CostType costType : values()) {
            if (costType.type == type) {
                return costType;
            }
        }
        throw new IllegalArgumentException("this type does not exists, type is: " + type);
    }

    CostType(int type, String name) {
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
