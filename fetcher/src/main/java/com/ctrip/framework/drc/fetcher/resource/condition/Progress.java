package com.ctrip.framework.drc.fetcher.resource.condition;

/**
 * @Author Slight
 * Sep 07, 2020
 */
public interface Progress {
    void tick();
    long get();
    void clear();
}
