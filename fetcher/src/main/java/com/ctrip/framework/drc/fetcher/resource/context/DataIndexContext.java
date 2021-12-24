package com.ctrip.framework.drc.fetcher.resource.context;

/**
 * @Author Slight
 * Jul 22, 2020
 */
public interface DataIndexContext extends Context.Simple, Context {

    String KEY_NAME = "data index";

    default void resetDataIndex() {
        updateDataIndex(0);
    }

    default int increaseDataIndexByOne() {
        return increaseDataIndex(1);
    }

    default int increaseDataIndex(int delta) {
        int before = fetchDataIndex();
        update(KEY_NAME, before + delta);
        return before;
    }

    default void updateDataIndex(int index) {
        update(KEY_NAME, index);
    }

    default int fetchDataIndex() {
        return (Integer) fetch(KEY_NAME);
    }
}
