package com.ctrip.framework.drc.core.server.config.applier.dto;

import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;

import static com.ctrip.framework.drc.core.server.common.enums.ConsumeType.Applier;
import static com.ctrip.framework.drc.core.server.common.enums.ConsumeType.Messenger;

/**
 * Created by jixinwang on 2021/9/14
 */
public enum ApplyMode {

    set_gtid(0, "set_gtid", Applier),
    transaction_table(1, "transaction_table", Applier),
    mq(2, "mq", Messenger);

    public static ApplyMode getApplyMode(final int type) {
        for(ApplyMode applyMode : values()) {
            if (applyMode.type == type) {
                return applyMode;
            }
        }
        return set_gtid;
    }

    ApplyMode(int type, String name, ConsumeType consumeType) {
        this.type = type;
        this.name = name;
        this.consumeType = consumeType;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }

    private int type;
    private String name;
    private ConsumeType consumeType;
}
