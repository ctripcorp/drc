package com.ctrip.framework.drc.replicator.impl.inbound.handler;

import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;

/**
 * @Author limingdong
 * @create 2020/2/26
 */
public class CharsetQueryCommandExecutor extends QueryCommandExecutor {

    public CharsetQueryCommandExecutor(CommandHandler commandHandler) {
        super(commandHandler);
    }

    @Override
    protected String getQueryString() {
        return "select @@character_set_database, @@collation_database;";
    }
}
