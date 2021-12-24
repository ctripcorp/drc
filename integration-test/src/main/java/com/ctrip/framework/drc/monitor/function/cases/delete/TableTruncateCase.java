package com.ctrip.framework.drc.monitor.function.cases.delete;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.monitor.function.cases.AbstractWriteCase;
import com.ctrip.framework.drc.monitor.function.cases.WriteCase;

/**
 * Created by mingdongli
 * 2019/11/17 上午11:10.
 */
public class TableTruncateCase extends AbstractWriteCase implements WriteCase {

    public TableTruncateCase(Execution execution) {
        super(execution);
    }
}
