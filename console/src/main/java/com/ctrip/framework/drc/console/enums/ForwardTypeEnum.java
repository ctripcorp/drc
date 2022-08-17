package com.ctrip.framework.drc.console.enums;

public enum ForwardTypeEnum {
    // for sha console forward to oversea
    BY_ARG(0),
    
    // for oversea forward to sha
    TO_CENTER(1);
    
    private int type;
    
    ForwardTypeEnum(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}
