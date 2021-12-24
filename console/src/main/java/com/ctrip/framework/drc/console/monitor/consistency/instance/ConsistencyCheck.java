package com.ctrip.framework.drc.console.monitor.consistency.instance;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

import java.util.Set;

/**
 * Created by mingdongli
 * 2019/11/15 下午5:03.
 */
public interface ConsistencyCheck extends Lifecycle {

    boolean check();

    Set<String> getDiff();
}
