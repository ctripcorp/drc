package com.ctrip.framework.drc.console.monitor.delay.impl.execution;

import com.ctrip.framework.drc.core.monitor.execution.SingleExecution;

import java.util.Collection;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-06
 */
public class GeneralSingleExecution implements SingleExecution {

    private String statement;

    public GeneralSingleExecution(String statement) {
        this.statement = statement;
    }


    public boolean replaceStatement(String statement) {
        this.statement = statement;
        return true;
    }

    @Override
    public String getStatement() {
        return this.statement;
    }

    @Override
    public Collection<String> getStatements() {
        return null;
    }

    @Override
    public boolean appendStatements(Collection<String> statement) {
        return false;
    }

    @Override
    public boolean replaceStatements(Collection<String> statement) {
        return false;
    }
}
