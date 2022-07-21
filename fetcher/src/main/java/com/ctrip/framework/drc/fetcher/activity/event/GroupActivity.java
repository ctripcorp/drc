package com.ctrip.framework.drc.fetcher.activity.event;

import com.ctrip.framework.drc.fetcher.event.ApplierDrcGtidEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.BaseBeginEvent;
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
        if (event instanceof BaseBeginEvent) {
            if (current != null) {
                logger.warn("BeginEvent (Last: UNKNOWN) received without TerminateEvent ahead. - ONLY ON RECONNECT");
                current.append(getRollbackEvent());
            }
            BaseBeginEvent b = (BaseBeginEvent) event;
            current = getTransaction(b);
            if (event instanceof ApplierDrcGtidEvent) {
                current.markTerminated();
                hand(current);
                current = null;
                return null;
            } else {
                return hand(current);
            }
        }
        current.append(event);
        if (event instanceof TerminateEvent) {
            current = null;
        }
        return null;
    }

    protected abstract Transaction getTransaction(BaseBeginEvent b);

    protected abstract TransactionEvent getRollbackEvent();
}
