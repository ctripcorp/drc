package com.ctrip.framework.drc.applier.mq;

/**
 * Created by jixinwang on 2022/10/17
 */
public enum MqType {

    QMQ,
    HERMES;

    public boolean isQmq() {
        return this.equals(MqType.QMQ);
    }

    public boolean isHermes() {
        return this.equals(MqType.HERMES);
    }
}
