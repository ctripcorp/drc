package com.ctrip.framework.drc.monitor.function.cases.select;

import com.ctrip.framework.drc.monitor.function.cases.AbstractReadCase;
import com.ctrip.framework.drc.monitor.function.cases.SelectCase;
import com.ctrip.framework.drc.core.monitor.execution.Execution;

/**
 * Created by mingdongli
 * 2019/10/9 下午10:45.
 */
public class SingleTableSelectCase extends AbstractReadCase implements SelectCase {

    public SingleTableSelectCase(Execution execution) {
        super(execution);
    }
}
