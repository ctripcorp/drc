package com.ctrip.framework.drc.fetcher.activity.event;

import com.ctrip.framework.drc.fetcher.event.FetcherEvent;
import com.ctrip.framework.drc.fetcher.event.FetcherRowsEvent;
import com.ctrip.framework.drc.fetcher.event.meta.MetaEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionEvent;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContext;
import com.ctrip.framework.drc.fetcher.system.InstanceActivity;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;

import java.util.concurrent.TimeUnit;

/**
 * @Author Slight
 * Jul 07, 2020
 */
public class InvolveActivity extends EventActivity<FetcherEvent, TransactionEvent> {

    @InstanceResource
    public LinkContext linkContext;

    @InstanceActivity
    public LoadEventActivity loadEventActivity;

    @Override
    protected void doInitialize() throws Exception {
        linkContext.resetTableKeyMap();
    }

    @Override
    public FetcherEvent doTask(FetcherEvent event) throws InterruptedException {
        if (event instanceof MetaEvent) {
            try {
                ((MetaEvent) event).involve(linkContext);
            } catch (Exception e) {
                logger.error("[" + linkContext.fetchGtid() + "]" + event.getClass().getSimpleName() + ".involve() FAIL - SLEEP 1 & RETRY", e);
                return retry(event, 1, TimeUnit.SECONDS);
            }
        }
        if (!(event instanceof TransactionEvent)) {
            return finish(event);
        }
        tryPreload(event);
        return hand((TransactionEvent) event);
    }

    protected void tryPreload(FetcherEvent event) {
        if (event instanceof FetcherRowsEvent) {
            if (loadEventActivity != null) {
                loadEventActivity.trySubmit((FetcherRowsEvent) event);
            }
        }
    }
}
