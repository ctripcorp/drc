package com.ctrip.framework.drc.fetcher.event.transaction;

import com.ctrip.framework.drc.fetcher.resource.transformer.TransformerContext;
import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;

/**
 * @Author Slight
 * Jul 08, 2020
 */
public interface TransactionData<T extends BaseTransactionContext> extends Traceable {

    enum ApplyResult {
        SUCCESS,
        CONFLICT_COMMIT,
        CONFLICT_ROLLBACK,
        WHATEVER_ROLLBACK,
        DEADLOCK,
        COMMUNICATION_FAILURE,
        LOAD,
        UNKNOWN,
        BATCH_ERROR,
    }

    default void transformer(TransformerContext transformerContext) {}

    ApplyResult apply(T context) throws InterruptedException;
}
