package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;

import java.util.List;
import java.util.Queue;

/**
 * @Author Slight
 * Sep 26, 2019
 */
public interface TransactionContext extends BaseTransactionContext {

    void begin();

    void rollback();

    void commit();

    void setGtid(String gtid);

    default void beginTransactionTable(String gtid) {}

    void recordTransactionTable(String gtid);

    TransactionData.ApplyResult complete();

    List<Boolean> getConflictMap();
    List<Boolean> getOverwriteMap();
    Queue<String> getLogs();
    Throwable getLastUnbearable();
}
