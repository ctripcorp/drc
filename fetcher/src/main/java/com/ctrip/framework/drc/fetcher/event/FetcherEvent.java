package com.ctrip.framework.drc.fetcher.event;

/**
 * @Author Slight
 * Sep 30, 2019
 */
public interface FetcherEvent extends AutoCloseable {

    //See confirmed.java8.DefaultOrSuper
    default void release() {
    }

    @Override
    default void close() {
        release();
    }
}
