package com.ctrip.framework.drc.core.server.common.enums;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public enum ConsumeType {

    Applier(0) {
        @Override
        public boolean shouldHeartBeat() {
            return true;
        }

        @Override
        public boolean isSlave() {
            return false;
        }
    }, // InstanceStatus.ACTIVE

    Slave(1) {
        @Override
        public boolean shouldHeartBeat() {
            return false;
        }

        @Override
        public boolean isSlave() {
            return true;
        }
    },  // InstanceStatus.INACTIVE

    Console(2) {
        @Override
        public boolean shouldHeartBeat() {
            return true;
        }

        @Override
        public boolean isSlave() {
            return false;
        }
    };

    private int code;

    ConsumeType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public abstract boolean shouldHeartBeat();

    public abstract boolean isSlave();

    public static ConsumeType getType(int code) {
        for (ConsumeType consumeType : values()) {
            if (consumeType.getCode() == code) {
                return consumeType;
            }
        }

        throw new UnsupportedOperationException("not support for code " + code);
    }
}