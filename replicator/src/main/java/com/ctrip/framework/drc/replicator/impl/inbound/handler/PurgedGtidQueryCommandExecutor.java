package com.ctrip.framework.drc.replicator.impl.inbound.handler;

import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;

/**
 * work for rds in aws and aliyun
 * @Author limingdong
 * @create 2022/12/12
 */
public class PurgedGtidQueryCommandExecutor extends QueryCommandExecutor {

    public PurgedGtidQueryCommandExecutor(CommandHandler commandHandler) {
        super(commandHandler);
    }

    @Override
    protected String getQueryString() {
        return "show global variables like \"gtid_purged\"";
    }
}
