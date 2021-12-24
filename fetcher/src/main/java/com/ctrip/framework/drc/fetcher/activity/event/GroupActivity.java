package com.ctrip.framework.drc.fetcher.activity.event;

import com.ctrip.framework.drc.fetcher.event.transaction.BeginEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TerminateEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.Transaction;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionEvent;

/**
 * @Author Slight
 * Jul 07, 2020
 */
public abstract class GroupActivity extends EventActivity<TransactionEvent, Transaction> {

    private Transaction current;

    @Override
    public TransactionEvent doTask(TransactionEvent event) throws InterruptedException {
        if (event instanceof BeginEvent) {
            if (current != null) {
                logger.warn("BeginEvent (Last: UNKNOWN) received without TerminateEvent ahead. - ONLY ON RECONNECT");
                current.append(getRollbackEvent());
            }
            BeginEvent b = (BeginEvent) event;
            current = getTransaction(b);
            return hand(current);
        }
        current.append(event);
        if (event instanceof TerminateEvent) {
            current = null;
        }
        return null;
    }

    protected abstract Transaction getTransaction(BeginEvent b);

    protected abstract TransactionEvent getRollbackEvent();
}
