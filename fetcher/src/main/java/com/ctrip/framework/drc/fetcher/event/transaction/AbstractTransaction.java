package com.ctrip.framework.drc.fetcher.event.transaction;

import com.ctrip.framework.drc.fetcher.event.FetcherEventGroup;
import com.ctrip.framework.drc.fetcher.resource.transformer.TransformerContext;
import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;

/**
 * @Author limingdong
 * @create 2021/3/25
 */
public abstract class AbstractTransaction<T extends BaseTransactionContext> extends FetcherEventGroup implements Transaction, TransactionData<T> {

    private TransformerContext transformerContext;

    public AbstractTransaction(TransactionEvent event) {
        super(event);
    }

    public void setTransformerResource(TransformerContext transformerContext) {
        this.transformerContext = transformerContext;
    }

    @Override
    public TransactionData.ApplyResult apply(T context) {
        try {
            context.initialize();
        } catch (Throwable t) {
            logger.warn("transaction context ({}) init fail: ", identifier(), t);
            return TransactionData.ApplyResult.COMMUNICATION_FAILURE;
        }
        try {
            reset();
            while (true) {
                TransactionEvent event = next();
                if (event == null)
                    break;
                event.transformer(transformerContext);
                TransactionData.ApplyResult result = event.apply(context);
                switch (result) {
                    case SUCCESS:
                        continue;
                    default:
                        return result;
                }
            }
            return TransactionData.ApplyResult.SUCCESS;
        } catch (Throwable t) {
            logger.error("UNLIKELY - fail to apply {}", identifier(), t);
            context.setLastUnbearable(t);
            return TransactionData.ApplyResult.UNKNOWN;
        } finally {
            context.mustDispose();
        }
    }
}
