package com.ctrip.framework.drc.fetcher.resource.context;

/**
 * @Author Slight
 * Oct 13, 2019
 */
public interface GtidContext extends Context.Simple, Context {

    String KEY_NAME = "gtid";

    String KEY_RESET = "unset";

    default void updateGtid(String gtid) {
        update(KEY_NAME, gtid);
    }

    default void resetGtid() {
        update(KEY_NAME, KEY_RESET);
    }

    default String fetchGtid() {
        return (String) fetch(KEY_NAME);
    }
}
