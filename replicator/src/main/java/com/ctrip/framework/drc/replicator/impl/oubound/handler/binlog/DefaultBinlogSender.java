package com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.utils.ScheduleCloseGate;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogSender;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.SchemaFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.sender.SenderFilterChainContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.sender.SenderFilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.sender.SenderOutboundLogEventContext;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.ApplierRegisterCommandHandler;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.ReplicatorMasterHandler;
import com.ctrip.framework.drc.replicator.store.manager.file.BinlogPosition;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.HEARTBEAT_LOGGER;

/**
 * @author yongnian
 */
public class DefaultBinlogSender extends AbstractLifecycle implements BinlogSender {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final ApplierRegisterCommandHandler applierRegisterCommandHandler;
    private final ScheduleCloseGate gate;
    private final Channel channel;
    private final ApplierDumpCommandPacket dumpCommandPacket;
    private final String applierName;
    private final String ip;
    private final ConsumeType consumeType;
    private final GtidSet excludedSet;
    private final DataMediaConfig dataMediaConfig;
    private final ChannelAttributeKey channelAttributeKey;
    private final Set<String> schemas;
    private final AviatorRegexFilter aviatorFilter;
    private Filter<OutboundLogEventContext> filterChain;
    private SenderOutboundLogEventContext senderContext = new SenderOutboundLogEventContext();
    private volatile boolean running = true;

    public DefaultBinlogSender(ApplierRegisterCommandHandler applierRegisterCommandHandler, Channel channel, ApplierDumpCommandPacket dumpCommandPacket) throws Exception {
        this.applierRegisterCommandHandler = applierRegisterCommandHandler;
        this.channel = channel;
        this.dumpCommandPacket = dumpCommandPacket;
        this.applierName = dumpCommandPacket.getApplierName();
        this.consumeType = ConsumeType.getType(dumpCommandPacket.getConsumeType());
        String properties = dumpCommandPacket.getProperties();
        this.dataMediaConfig = DataMediaConfig.from(applierName, properties);
        this.ip = ApplierRegisterCommandHandler.getIpFromChannel(channel);
        logger.info("[ConsumeType] is {}, [properties] is {}, [replicatorRegion] is {}, [applierRegion] is {}, for {} from {}", consumeType.name(), properties, applierRegisterCommandHandler.getReplicatorRegion(), dumpCommandPacket.getRegion(), applierName, channel.remoteAddress());
        channelAttributeKey = channel.attr(ReplicatorMasterHandler.KEY_CLIENT).get();
        if (!consumeType.shouldHeartBeat()) {
            channelAttributeKey.setHeartBeat(false);
            HEARTBEAT_LOGGER.info("[HeartBeat] stop due to replicator slave for {}:{}", applierName, channel.remoteAddress().toString());
        }
        this.gate = channelAttributeKey.getGate();
        this.excludedSet = dumpCommandPacket.getGtidSet().clone();
        logger.info("[SCHEMA][ADD] for {}, nameFilter {}", applierName, dumpCommandPacket.getNameFilter());
        this.schemas = SchemaFilter.getSchemas(dumpCommandPacket.getNameFilter());
        this.aviatorFilter = new AviatorRegexFilter(dumpCommandPacket.getNameFilter());

        logger.info("[SCHEMA][ADD] for {}, nameFilter {}, schema {}", applierName, dumpCommandPacket.getNameFilter(), schemas);
    }

    @Override
    protected void doInitialize() throws Exception {
        this.addListener();
        this.filterChain = new SenderFilterChainFactory().createFilterChain(
                SenderFilterChainContext.from(
                        this,
                        this.channel,
                        this.dataMediaConfig,
                        applierRegisterCommandHandler.getOutboundMonitorReport(),
                        applierRegisterCommandHandler.getReplicatorRegion(),
                        dumpCommandPacket.getRegion()
                )
        );
        this.senderContext = new SenderOutboundLogEventContext();
    }

    @Override
    protected void doStart() throws Exception {
        this.running = true;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean concernSchema(String dbName) {
        return schemas.contains(dbName);
    }

    @Override
    public boolean concernTable(String tableName) {
        return aviatorFilter.filter(tableName);
    }

    @Override
    public BinlogPosition getBinlogPosition() {
        return senderContext.getBinlogPosition();
    }

    private void addListener() {
        this.channel.closeFuture().addListener((ChannelFutureListener) future -> {
            running = false;
            Throwable throwable = future.cause();
            if (throwable != null) {
                logger.error("Sender closeFuture", throwable);
            }
            channelAttributeKey.getGate().open();
            removeListener();
            logger.info("{} closeFuture Listener invoke open gate {} and set channelClosed", applierName, gate);
        });
    }


    private void removeListener() {
        NettyClient nettyClient = applierRegisterCommandHandler.removeApplierKeys(new ApplierRegisterCommandHandler.ApplierKey(applierName, ip));
        if (nettyClient != null && nettyClient.channel().isActive()) {
            nettyClient.channel().close();
        }
    }

    @Override
    public void doDispose() {
        channel.close();
    }

    @Override
    public void send(OutboundLogEventContext context) throws Exception {
        gate.tryPass();
        if (!running) {
            logger.info("{} not running, stop send.", applierName);
            return;
        }
        boolean refresh = senderContext.refresh(context);
        if (!refresh) {
            return;
        }
        filterChain.doFilter(senderContext);
        if (senderContext.getCause() != null) {
            logger.error("{} sender error, close.", applierName, senderContext.getCause());
            this.dispose();
        }
    }

    @Override
    public void updatePosition(BinlogPosition binlogPosition) {
        senderContext.updatePosition(binlogPosition);
    }

    @Override
    public void refreshInExcludedGroup(OutboundLogEventContext scannerContext) {
        if (senderContext.getBinlogPosition().canMoveForward(scannerContext.getBinlogPosition())) {
            senderContext.setInExcludeGroup(scannerContext.isInExcludeGroup());
        }
    }

    @Override
    public ConsumeType getConsumeType() {
        return consumeType;
    }

    @Override
    public String getNameFilter() {
        return dumpCommandPacket.getNameFilter();
    }

    @Override
    public String getApplierName() {
        return applierName;
    }

    @Override
    public ChannelAttributeKey getChannelAttributeKey() {
        return channelAttributeKey;
    }

    @Override
    public void send(ResultCode resultCode) {
        resultCode.sendResultCode(channel, logger);
    }

    @Override
    public GtidSet getGtidSet() {
        return excludedSet;
    }

    @Override
    public String toString() {
        return applierName + getBinlogPosition();
    }

    @VisibleForTesting
    public SenderOutboundLogEventContext getSenderContext() {
        return senderContext;
    }

    @VisibleForTesting
    public Filter<OutboundLogEventContext> getFilterChain() {
        return filterChain;
    }

    @Override
    public Channel getChannel() {
        return channel;
    }
}
