package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.event.ApplierRollbackEvent;
import com.ctrip.framework.drc.applier.event.ApplierTransaction;
import com.ctrip.framework.drc.fetcher.activity.event.GroupActivity;
import com.ctrip.framework.drc.fetcher.event.transaction.BeginEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.Transaction;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionEvent;

/**
 * @Author limingdong
 * @create 2021/3/4
 */
public class ApplierGroupActivity extends GroupActivity {

    @Override
    protected Transaction getTransaction(BeginEvent b) {
        return new ApplierTransaction((com.ctrip.framework.drc.applier.event.transaction.BeginEvent) b);
    }

    @Override
    protected TransactionEvent getRollbackEvent() {
        return new ApplierRollbackEvent();
    }
}
