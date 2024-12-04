package com.ctrip.framework.drc.messenger.event;

import com.ctrip.framework.drc.fetcher.event.MonitoredDeleteRowsEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionContext;

/**
 * @Author Slight
 * Oct 24, 2019
 */
public class MessengerDeleteRowsEvent extends MonitoredDeleteRowsEvent<TransactionContext> {

}
