package com.ctrip.framework.drc.fetcher.resource.thread;

/**
 * @Author Slight
 * Sep 24, 2019
 */
public interface Executor {

    void execute(Runnable runnable);
}
