package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.event.transaction.Transaction;
import com.ctrip.framework.drc.fetcher.resource.condition.LWM;
import com.ctrip.framework.drc.fetcher.activity.event.EventActivity;
import com.ctrip.framework.drc.fetcher.activity.event.ReleaseActivity;
import com.ctrip.framework.drc.fetcher.resource.condition.Capacity;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;

/**
 * @Author Slight
 * Jul 08, 2020
 */
public class CommitActivity extends EventActivity<Transaction, Boolean> implements ReleaseActivity {

    @InstanceResource
    public LWM lwm;

    @InstanceResource
    public Capacity capacity;

    @InstanceConfig(path = "registryKey")
    public String registryKey;

    @Override
    public Transaction doTask(Transaction transaction) throws InterruptedException {
        loggerTL.info("[{}] commit {}", registryKey, transaction.identifier());
        transaction.commit(lwm);
        release();
        return finish(transaction);
    }

    @Override
    public void release() {
        capacity.release();
    }
}
