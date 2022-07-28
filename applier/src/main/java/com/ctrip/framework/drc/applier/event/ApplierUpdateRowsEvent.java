package com.ctrip.framework.drc.applier.event;

import com.ctrip.framework.drc.fetcher.event.transaction.TransactionContext;
import com.ctrip.framework.drc.fetcher.event.MonitoredUpdateRowsEvent;
import com.ctrip.framework.drc.fetcher.event.meta.MetaEvent;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContext;

/**
 * @Author Slight
 * Oct 17, 2019
 */
public class ApplierUpdateRowsEvent extends MonitoredUpdateRowsEvent<TransactionContext> implements MetaEvent.Read<LinkContext> {

}
