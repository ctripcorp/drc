package com.ctrip.framework.drc.replicator.impl.inbound.handler;

import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;

/**
 * Created by mingdongli
 * 2019/9/20 上午11:58.
 */
public class CheckSumQueryCommandExecutor extends QueryCommandExecutor {

    public CheckSumQueryCommandExecutor(CommandHandler commandHandler) {
        super(commandHandler);
    }

    @Override
    protected String getQueryString() {
        return "select @@global.binlog_checksum";
    }
}
