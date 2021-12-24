package com.ctrip.framework.drc.validation.event;

import com.ctrip.framework.drc.fetcher.event.MonitoredDeleteRowsEvent;
import com.ctrip.framework.drc.validation.resource.context.ValidationTransactionContext;

/**
 * @author: Haibo Shen
 * @Date: 2021/3/17
 */
public class ValidationDeleteRowsEvent extends MonitoredDeleteRowsEvent<ValidationTransactionContext> {

}

