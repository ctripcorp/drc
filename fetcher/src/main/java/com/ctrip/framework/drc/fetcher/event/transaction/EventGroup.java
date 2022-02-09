package com.ctrip.framework.drc.fetcher.event.transaction;

/**
 * @Author Slight
 * Jul 07, 2020
 */
public interface EventGroup {

    void append(TransactionEvent event) throws InterruptedException;

    TransactionEvent next() throws InterruptedException;

    boolean isOverflowed() throws InterruptedException;

    void reset();

    boolean isEmptyTransaction();
}
