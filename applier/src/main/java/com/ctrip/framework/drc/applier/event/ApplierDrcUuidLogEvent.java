package com.ctrip.framework.drc.applier.event;

import com.ctrip.framework.drc.core.driver.binlog.impl.DrcUuidLogEvent;
import com.ctrip.framework.drc.fetcher.event.FetcherEvent;

/**
 * Created by jixinwang on 2023/12/11
 */
public class ApplierDrcUuidLogEvent extends DrcUuidLogEvent implements FetcherEvent {
}
