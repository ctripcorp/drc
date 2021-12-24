package com.ctrip.framework.drc.monitor.function.cases.config;

import com.ctrip.framework.drc.monitor.function.cases.AbstractWriteCase;
import com.ctrip.framework.drc.monitor.function.cases.WriteCase;
import com.ctrip.framework.drc.core.monitor.execution.Execution;

/**
 * Created by mingdongli
 * 2019/10/12 下午2:17.
 */
public class VariableSettingCase extends AbstractWriteCase implements WriteCase {

    public VariableSettingCase(Execution execution) {
        super(execution);
    }
}
