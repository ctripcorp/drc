package com.ctrip.framework.drc.monitor.function.execution;

import com.ctrip.framework.drc.core.monitor.execution.Execution;

/**
 * Created by mingdongli
 * 2019/10/9 下午11:39.
 */
public interface WriteExecution extends Execution {

    int affect();
}
