package com.ctrip.framework.drc.console.monitor.consistency.sql.execution;

import com.ctrip.framework.drc.console.monitor.consistency.table.KeyAware;
import com.ctrip.framework.drc.console.monitor.consistency.table.OnUpdateAware;
import com.ctrip.framework.drc.console.monitor.consistency.table.TableNameAware;
import com.ctrip.framework.drc.core.monitor.execution.SingleExecution;

/**
 * Created by mingdongli
 * 2019/11/15 上午10:44.
 */
public interface NameAwareExecution extends SingleExecution, KeyAware, TableNameAware, OnUpdateAware {

}
