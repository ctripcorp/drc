package com.ctrip.framework.drc.console.monitor.delay.impl.driver;

import com.ctrip.framework.drc.console.config.ConsoleConfig;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorSlaveConfig;
import com.ctrip.framework.drc.console.monitor.delay.impl.convertor.DelayMonitorByteBufConverter;
import com.ctrip.framework.drc.console.monitor.delay.impl.handler.command.DelayMonitorCommandHandler;
import com.ctrip.framework.drc.core.config.RegionConfig;
import com.ctrip.framework.drc.core.driver.AbstractInstanceConnection;
import com.ctrip.framework.drc.core.driver.MySQLConnection;
import com.ctrip.framework.drc.core.driver.MySQLConnector;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.command.packet.monitor.DelayMonitorCommandPacket;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.ObjectPoolException;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-11-28
 */
public class DelayMonitorConnection extends AbstractInstanceConnection implements MySQLConnection {

    private final Logger logger = LoggerFactory.getLogger("delayMonitorLogger");

    private DelayMonitorSlaveConfig config;

    private static final String CLOG_TAGS = "[[monitor=delay,direction={}({}):{}({}),cluster={},replicator={}:{},measurement={}]]";

    private static final String WARN = "warn";

    private static final String ERROR = "error";

    private static final String DEBUG = "debug";

    private static final String INFO= "info";

    public DelayMonitorConnection(MySQLSlaveConfig mySQLSlaveConfig, LogEventHandler eventHandler, MySQLConnector connector) {
        super(mySQLSlaveConfig, eventHandler, connector);
        config = (DelayMonitorSlaveConfig) mySQLSlaveConfig;
    }

    @Override
    protected void doWithSimpleObjectPool(SimpleObjectPool<NettyClient> simpleObjectPool, DumpCallBack callBack, RECONNECTION_CODE reconnectionCode) {
        log("DUMP", INFO, null);
        CommandFuture<ResultCode> commandFuture = doExecuteCommand(simpleObjectPool);
        commandFuture.addListener(new CommandFutureListener<ResultCode>() {
            @Override
            public void operationComplete(CommandFuture<ResultCode> commandFuture) {
                Throwable throwable = commandFuture.cause();
                if (throwable != null) {
                    log("CommandFuture error", ERROR, throwable);
                }
                reconnect(simpleObjectPool, callBack, reconnectionCode);
            }
        });
    }

    private void reconnect(SimpleObjectPool<NettyClient> simpleObjectPool, DumpCallBack callBack, RECONNECTION_CODE reconnection_code) {
        try {
            log("simpleObjectPool clear", INFO, null);
            simpleObjectPool.clear();
        } catch (ObjectPoolException e) {
            log("simpleObjectPool clear", ERROR, e);
        }
        log("pending to reconnect, for 1 seconds", INFO, null);
        scheduleReconnect(callBack, reconnection_code);
    }

    protected CommandFuture<ResultCode> doExecuteCommand(SimpleObjectPool<NettyClient> simpleObjectPool) {
        DelayMonitorCommandHandler delayMonitorCommandHandler = new DelayMonitorCommandHandler(logEventHandler, new DelayMonitorByteBufConverter());
        DelayMonitorCommandPacket commandPacket = new DelayMonitorCommandPacket(SERVER_COMMAND.COM_DELAY_MONITOR.getCode());
        commandPacket.setDcName(config.getDc());
        commandPacket.setClusterName(config.getCluster());
        commandPacket.setRegion(RegionConfig.getInstance().getRegionForDc(config.getDc()));
        CommandFuture<ResultCode> commandFuture = delayMonitorCommandHandler.handle(commandPacket, simpleObjectPool);
        return commandFuture;
    }

    private void log(String msg, String types, Throwable t) {
        String prefix = new StringBuilder().append(CLOG_TAGS).append(msg).toString();
        switch (types) {
            case WARN:
                logger.warn(prefix, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement());
                break;
            case ERROR:
                logger.error(prefix, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement(), t);
                break;
            case DEBUG:
                logger.debug(prefix, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement());
                break;
            case INFO:
                logger.info(prefix, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement());
                break;
        }
    }
}
