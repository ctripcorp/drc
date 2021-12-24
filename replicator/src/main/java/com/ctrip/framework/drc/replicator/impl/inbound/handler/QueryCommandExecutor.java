package com.ctrip.framework.drc.replicator.impl.inbound.handler;

import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.packet.client.QueryCommandPacket;
import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;

import java.util.Collections;
import java.util.List;

/**
 * Created by mingdongli
 * 2019/9/19 下午11:38.
 */
public abstract class QueryCommandExecutor extends AbstractCommandExecutor implements CommandExecutor {

    public QueryCommandExecutor(CommandHandler commandHandler) {
        super(commandHandler);
    }

    @Override
    public List<String> getCommand() {
        return Collections.singletonList(getQueryString());
    }

    protected abstract String getQueryString();

    @Override
    protected ServerCommandPacket getPacket(String queryString) {
        return new QueryCommandPacket(queryString);
    }

}
