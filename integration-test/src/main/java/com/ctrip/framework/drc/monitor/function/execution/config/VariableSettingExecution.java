package com.ctrip.framework.drc.monitor.function.execution.config;

import com.ctrip.framework.drc.monitor.function.execution.AbstractExecution;
import com.ctrip.framework.drc.core.monitor.execution.Execution;

import java.util.List;

/**
 * Created by mingdongli
 * 2019/10/12 下午2:14.
 */
public class VariableSettingExecution extends AbstractExecution implements Execution {

    public VariableSettingExecution(List<String> statements) {
        this.statements = statements;
    }
}
