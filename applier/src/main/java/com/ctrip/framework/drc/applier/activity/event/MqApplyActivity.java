package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.event.transaction.Transaction;
import com.ctrip.framework.drc.applier.resource.context.MqTransactionContextResource;
import com.ctrip.framework.drc.applier.resource.mysql.DataSource;
import com.ctrip.framework.drc.fetcher.activity.event.EventActivity;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;

/**
 * Created by jixinwang on 2022/10/12
 */
public class MqApplyActivity extends EventActivity<Transaction, Transaction> {

    @InstanceResource
    public DataSource dataSource;

    @InstanceConfig(path = "registryKey")
    public String registryKey;

    public MqTransactionContextResource mq;

    @Override
    protected void doInitialize() throws Exception {
        mq = (MqTransactionContextResource) derive(MqTransactionContextResource.class);
    }

    @Override
    public Transaction doTask(Transaction transaction) throws InterruptedException {

        loggerTL.info("[{}] apply {}", registryKey, transaction.identifier());
        boolean bigTransaction = transaction.isOverflowed();

        switch (transaction.apply(mq)) {
            case SUCCESS:
                return onSuccess(transaction);
            default:
                return onSuccess(transaction);
        }
    }

    protected Transaction onSuccess(Transaction transaction) throws InterruptedException {
        return hand(transaction);
    }
}
