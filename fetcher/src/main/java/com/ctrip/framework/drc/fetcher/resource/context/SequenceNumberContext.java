package com.ctrip.framework.drc.fetcher.resource.context;

/**
 * @Author Slight
 * Oct 22, 2019
 */
public interface SequenceNumberContext extends Context.Simple {

    String KEY_NAME = "sequence number";

    default void updateSequenceNumber(long sequenceNumber) {
        update(KEY_NAME, sequenceNumber);
    }

    default long fetchSequenceNumber() {
        return (long) fetch(KEY_NAME);
    }
}
