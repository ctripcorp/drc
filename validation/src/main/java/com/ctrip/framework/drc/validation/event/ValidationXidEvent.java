package com.ctrip.framework.drc.validation.event;

import com.ctrip.framework.drc.fetcher.event.MonitoredXidEvent;
import com.ctrip.framework.drc.fetcher.event.meta.MetaEvent;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemoryAware;
import com.ctrip.framework.drc.fetcher.resource.context.EventGroupContext;
import com.ctrip.framework.drc.validation.resource.context.ValidationTransactionContext;

/**
 * @author: Haibo Shen
 * @Date: 2021/3/17
 */
public class ValidationXidEvent extends MonitoredXidEvent<ValidationTransactionContext> implements MetaEvent.Write<EventGroupContext>, DirectMemoryAware {
}
