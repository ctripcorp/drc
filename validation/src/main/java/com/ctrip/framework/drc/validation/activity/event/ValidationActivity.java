package com.ctrip.framework.drc.validation.activity.event;

import com.ctrip.framework.drc.fetcher.activity.event.EventActivity;
import com.ctrip.framework.drc.fetcher.activity.event.ReleaseActivity;
import com.ctrip.framework.drc.fetcher.resource.condition.Capacity;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import com.ctrip.framework.drc.validation.event.ValidationTransaction;
import com.ctrip.framework.drc.validation.resource.context.ValidationTransactionContextResource;

/**
 * @author: Haibo Shen
 * @Date: 2021/3/16
 */
public class ValidationActivity extends EventActivity<ValidationTransaction, ValidationTransaction> implements ReleaseActivity {

    public ValidationTransactionContextResource context;

    @InstanceResource
    public Capacity capacity;

    @Override
    protected void doInitialize() throws Exception {
        context = (ValidationTransactionContextResource) derive(ValidationTransactionContextResource.class);
    }

    @Override
    public ValidationTransaction doTask(ValidationTransaction transaction) {
        try {
            logger.info("[ValidationActivity]apply {}, {}", transaction.identifier());
            transaction.apply(context);
            return finish(transaction);
        } finally {
            release();
        }
    }

    @Override
    public void release() {
        capacity.release();
    }
}
