package com.ctrip.framework.drc.replicator.store;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;

import java.io.Flushable;

/**
 * Created by mingdongli
 * 2019/9/18 上午12:11.
 */
public interface EventWriter extends IoCache, Flushable, Destroyable, Lifecycle {

}
