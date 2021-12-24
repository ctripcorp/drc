package com.ctrip.framework.drc.applier.resource.condition;

/**
 * @Author Slight
 * Sep 24, 2019
 */
public interface LWM {
    void acquire(long sequenceNumber) throws InterruptedException;
    void commit(long sequenceNumber) throws InterruptedException;

    boolean tryPass(long lastCommitted) throws InterruptedException;
    void onCommit(long lastCommitted, LWMPassHandler handler, String... identifier) throws InterruptedException;

    long current();
}
