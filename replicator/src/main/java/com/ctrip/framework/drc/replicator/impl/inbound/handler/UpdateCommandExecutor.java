package com.ctrip.framework.drc.replicator.impl.inbound.handler;

import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.packet.client.QueryCommandPacket;
import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.MASTER_HEARTBEAT_PERIOD_SECONDS;

/**
 * Created by mingdongli
 * 2019/9/19 下午11:21.
 */
public class UpdateCommandExecutor extends AbstractCommandExecutor implements CommandExecutor {


    public UpdateCommandExecutor(CommandHandler commandHandler) {
        super(commandHandler);
    }

    @Override
    public List<String> getCommand() {
        List<String> commands = new ArrayList<>();
        commands.add("set wait_timeout=9999999");
        commands.add("set net_write_timeout=1800");
        commands.add("set net_read_timeout=1800");
        commands.add("set names 'binary'");
        commands.add("set @master_binlog_checksum= @@global.binlog_checksum");
        commands.add("set @slave_uuid=uuid()");
        long periodNano = TimeUnit.SECONDS.toNanos(MASTER_HEARTBEAT_PERIOD_SECONDS);
        commands.add("SET @master_heartbeat_period=" + periodNano);
        return commands;
    }

    @Override
    protected ServerCommandPacket getPacket(String queryString) {
        return new QueryCommandPacket(queryString);
    }

}
