package com.ctrip.framework.drc.validation.event;

import com.ctrip.framework.drc.fetcher.event.MonitoredWriteRowsEvent;
import com.ctrip.framework.drc.fetcher.event.meta.MetaEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionEvent;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContext;
import com.ctrip.framework.drc.validation.resource.context.ValidationTransactionContext;

/**
 * @author: Haibo Shen
 * @Date: 2021/3/17
 */
public class ValidationWriteRowsEvent extends MonitoredWriteRowsEvent<ValidationTransactionContext> implements TransactionEvent<ValidationTransactionContext>, MetaEvent.Read<LinkContext> {

}
