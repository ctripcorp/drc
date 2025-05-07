package com.ctrip.framework.drc.fetcher.activity.event;

import com.ctrip.framework.drc.fetcher.event.transaction.ApplyTransaction;
import com.ctrip.framework.drc.fetcher.resource.condition.LWM;
import com.ctrip.framework.drc.fetcher.resource.condition.LWMPassHandler;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;

/**
 * @Author Slight
 * Jul 07, 2020
 */
public class DispatchActivity extends EventActivity<ApplyTransaction, ApplyTransaction> {

    @InstanceResource
    public LWM lwm;

    @InstanceConfig(path = "registryKey")
    public String registryKey;

    @Override
    public ApplyTransaction doTask(ApplyTransaction transaction) throws InterruptedException {
        loggerTL.info("[{}] dispatch {}", registryKey, transaction.identifier());
        transaction.begin(lwm, new BeginHandler(transaction, this::hand));
        return null;
    }


    @FunctionalInterface
    private interface DispatchFunction {
        void onBegin(ApplyTransaction transaction) throws InterruptedException;
    }

    private static class BeginHandler implements LWMPassHandler {

        private final ApplyTransaction transaction;
        private final DispatchFunction func;

        private BeginHandler(ApplyTransaction transaction, DispatchFunction func) {
            this.transaction = transaction;
            this.func = func;
        }

        @Override
        public void onBegin() throws InterruptedException {
            func.onBegin(transaction);
        }

        @Override
        public void close() {
            try {
                transaction.close();
            } catch (Exception e) {
                loggerTL.warn("close transaction fail ", e);
            }
        }
    }
}
