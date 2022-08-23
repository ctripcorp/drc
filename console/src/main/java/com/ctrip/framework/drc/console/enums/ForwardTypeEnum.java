package com.ctrip.framework.drc.console.enums;

public enum ForwardTypeEnum {
    // for sha console forward to oversea
    TO_OVERSEA_BY_ARG(0),
    
    // for oversea forward to sha
    TO_META_DB(1);
    
    private int type;
    
    ForwardTypeEnum(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}
