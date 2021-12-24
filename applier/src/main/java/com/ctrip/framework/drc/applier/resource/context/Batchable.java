package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.xpipe.api.lifecycle.Disposable;

/**
 * @Author limingdong
 * @create 2021/2/1
 */
public interface Batchable extends Disposable {

    TransactionData.ApplyResult executeBatch();

}
