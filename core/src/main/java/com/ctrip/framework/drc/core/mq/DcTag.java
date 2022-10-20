package com.ctrip.framework.drc.core.mq;

/**
 * Created by jixinwang on 2022/10/19
 */
public enum DcTag {

    LOCAL(0, "local"),
    PEER(1, "peer");

    public static DcTag getApplyMode(final int type) {
        for(DcTag dcTag : values()) {
            if (dcTag.type == type) {
                return dcTag;
            }
        }
        throw new IllegalArgumentException("type is not exist: "+ type);
    }

    DcTag(int type, String name) {
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
