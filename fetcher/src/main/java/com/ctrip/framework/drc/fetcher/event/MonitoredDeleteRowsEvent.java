package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;

import java.util.List;

/**
 * @author: Haibo Shen
 * @Date: 2021/3/17
 */
public class MonitoredDeleteRowsEvent<T extends BaseTransactionContext> extends FetcherRowsEvent<T> {

    public MonitoredDeleteRowsEvent() {
        DefaultEventMonitorHolder.getInstance().logBatchEvent("event", "delete rows", 1, 0);
    }

    @Override
    public String identifier() {
        try {
            return gtid + "-" + dataIndex + "-D";
        } catch (Throwable t) {
            return getClass().getSimpleName() + ":UNKNOWN";
        }
    }

    @Override
    protected void doApply(T context) {
        List<List<Object>> beforeRows = getBeforePresentRowsValues();
        Bitmap beforeBitmap = Bitmap.from(getBeforeRowsKeysPresent());
        loggerR.info(attachTags(context,"DELETE.doApply()" +
                "\nbefore rows: " + beforeRows +
                "\nbefore bitmap: " + beforeBitmap));
        context.delete(beforeRows, beforeBitmap, columns);
    }
}
