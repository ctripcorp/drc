package com.ctrip.framework.drc.replicator.impl.inbound.handler;

import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;

/**
 * Created by mingdongli
 * 2019/9/20 上午11:59.
 */
public class MasterStatusQueryCommandExecutor extends QueryCommandExecutor {

    public MasterStatusQueryCommandExecutor(CommandHandler commandHandler) {
        super(commandHandler);
    }

    @Override
    protected String getQueryString() {
        return "show master status";
    }
}
