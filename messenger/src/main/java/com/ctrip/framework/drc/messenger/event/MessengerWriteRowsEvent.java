package com.ctrip.framework.drc.messenger.event;

import com.ctrip.framework.drc.fetcher.event.MonitoredWriteRowsEvent;
import com.ctrip.framework.drc.fetcher.event.meta.MetaEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionContext;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionEvent;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContext;

/**
 * @Author Slight
 * Sep 26, 2019
 */
public class MessengerWriteRowsEvent extends MonitoredWriteRowsEvent<TransactionContext> implements TransactionEvent<TransactionContext>, MetaEvent.Read<LinkContext> {

}
