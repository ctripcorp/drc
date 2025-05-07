package com.ctrip.framework.drc.fetcher.activity.event;

import com.ctrip.framework.drc.fetcher.event.transaction.ApplyTransaction;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.SystemStatus;

import java.util.concurrent.TimeUnit;


/**
 * Created by shiruixin
 * 2024/10/28 14:10
 */
public abstract class BaseApplyActivity extends EventActivity<ApplyTransaction, ApplyTransaction> {
    @InstanceConfig(path = "registryKey")
    public String registryKey;

    protected boolean handleEmptyTransaction(ApplyTransaction transaction) throws InterruptedException {
        return false;
    }

    protected ApplyTransaction onSuccess(ApplyTransaction transaction) throws InterruptedException {
        return hand(transaction);
    }

    protected ApplyTransaction onFailure(ApplyTransaction transaction) throws InterruptedException {
        logger.error("apply failed: {}, shutdown server", registryKey);
        logger.info("apply activity status is stopped for {}", registryKey);
        getSystem().setStatus(SystemStatus.STOPPED);
        return finish(transaction);
    }

    protected ApplyTransaction onDeadLock(ApplyTransaction transaction) throws InterruptedException {
        return retry(transaction, TimeUnit.SECONDS, 0, 1, 5, 10, (trx) -> {
            logger.error("- UNLIKELY - task ({}) retries exceeds limit.", transaction.identifier());
            return onFailure(trx);
        });
    }
}
