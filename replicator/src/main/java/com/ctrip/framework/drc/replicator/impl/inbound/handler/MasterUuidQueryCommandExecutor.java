package com.ctrip.framework.drc.replicator.impl.inbound.handler;

import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;

/**
 * Created by mingdongli
 * 2019/10/15 下午8:19.
 */
public class MasterUuidQueryCommandExecutor extends QueryCommandExecutor {

    public MasterUuidQueryCommandExecutor(CommandHandler commandHandler) {
        super(commandHandler);
    }

    @Override
    protected String getQueryString() {
        return "SHOW GLOBAL VARIABLES LIKE 'server_uuid'";
    }
}
