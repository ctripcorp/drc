package com.ctrip.framework.drc.fetcher.event.transaction;

/**
 * @Author Slight
 * Jul 08, 2020
 */
public interface Traceable {

    default String identifier() {
        return getClass().getSimpleName();
    }
}
