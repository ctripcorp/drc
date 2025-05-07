package com.ctrip.framework.drc.messenger.activity.event;

import com.ctrip.framework.drc.core.driver.binlog.gtid.Gtid;
import com.ctrip.framework.drc.fetcher.activity.event.BaseApplyActivity;
import com.ctrip.framework.drc.fetcher.event.transaction.ApplyTransaction;
import com.ctrip.framework.drc.fetcher.resource.context.MqPosition;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import com.ctrip.framework.drc.messenger.resource.context.MqTransactionContextResource;

import java.util.concurrent.TimeUnit;

/**
 * Created by jixinwang on 2022/10/12
 */
public class MqApplyActivity extends BaseApplyActivity {

    public MqTransactionContextResource transactionContext;

    @InstanceResource
    public MqPosition mqPosition;

    @Override
    protected void doInitialize() throws Exception {
        transactionContext = (MqTransactionContextResource) derive(MqTransactionContextResource.class);
    }

    @Override
    public ApplyTransaction doTask(ApplyTransaction transaction) throws InterruptedException {
        loggerTL.info("[{}] apply {}", registryKey, transaction.identifier());

        switch (transaction.apply(transactionContext)) {
            case SUCCESS:
                return onSuccess(transaction);
            default:
                return onRetry(transaction);
        }
    }

    @Override
    protected ApplyTransaction onSuccess(ApplyTransaction transaction) throws InterruptedException {
        mqPosition.add(new Gtid(transactionContext.fetchGtid()));
        return super.onSuccess(transaction);
    }

    protected ApplyTransaction onRetry(ApplyTransaction transaction) throws InterruptedException {
        return retry(transaction, TimeUnit.SECONDS, 0, 1, 2, 3, (trx) -> {
            logger.error("- MQ UNLIKELY - task ({}) retries exceeds limit.", transaction.identifier());
            return super.onFailure(trx);
        });
    }
}
