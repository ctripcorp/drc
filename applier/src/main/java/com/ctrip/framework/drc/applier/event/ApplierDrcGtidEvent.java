package com.ctrip.framework.drc.applier.event;


import com.ctrip.framework.drc.applier.resource.context.TransactionContext;
import com.ctrip.framework.drc.fetcher.event.MonitoredGtidLogEvent;

/**
 * Created by jixinwang on 2021/9/23
 */
public class ApplierDrcGtidEvent extends MonitoredGtidLogEvent<TransactionContext> {
}
