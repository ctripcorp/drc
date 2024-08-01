package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.fetcher.event.meta.MetaEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionEvent;
import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContext;

import java.util.List;

/**
 * @author: Haibo Shen
 * @Date: 2021/3/17
 */
public class MonitoredUpdateRowsEvent<T extends BaseTransactionContext> extends FetcherRowsEvent<T> implements TransactionEvent<T>, MetaEvent.Read<LinkContext> {

    @Override
    public String identifier() {
        try {
            return gtid + "-" + dataIndex + "-U";
        } catch (Throwable t) {
            return getClass().getSimpleName() + ":UNKNOWN";
        }
    }

    @Override
    protected void doApply(T context) {
        List<List<Object>> afterRows = getAfterPresentRowsValues();
        Bitmap afterBitmap = Bitmap.from(getAfterRowsKeysPresent());
        List<List<Object>> beforeRows = getBeforePresentRowsValues();
        Bitmap beforeBitmap = Bitmap.from(getBeforeRowsKeysPresent());
        if (loggerR.isDebugEnabled()) {
            loggerR.debug(attachTags(context, "UPDATE.doApply()" +
                    "\nafter rows: " + afterRows +
                    "\nafter bitmap: " + afterBitmap +
                    "\nbefore rows: " + beforeRows +
                    "\nbefore bitmap: " + beforeBitmap));
        }
        context.update(beforeRows, beforeBitmap,
                afterRows, afterBitmap, columns);
    }
}