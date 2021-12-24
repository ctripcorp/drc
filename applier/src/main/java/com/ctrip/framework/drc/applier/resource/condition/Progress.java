package com.ctrip.framework.drc.applier.resource.condition;

/**
 * @Author Slight
 * Sep 07, 2020
 */
public interface Progress {
    void tick();
    long get();
    void clear();
}
