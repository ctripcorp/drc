package com.ctrip.framework.drc.replicator.impl.oubound.handler;

import com.ctrip.framework.drc.core.config.RegionConfig;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.replicator.impl.oubound.ReplicatorException;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogScannerManager;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog.DefaultBinlogScannerManager;
import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import static com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND.COM_APPLIER_BINLOG_DUMP_GTID;


/**
 * deal with ApplierDumpCommandPacket
 * Created by mingdongli
 * 2019/9/21 10:13
 */
public class ApplierRegisterCommandHandler extends AbstractServerCommandHandler implements CommandHandler {

    private GtidManager gtidManager;

    private FileManager fileManager;

    private OutboundMonitorReport outboundMonitorReport;

    private String replicatorRegion;

    private String replicatorName;

    private ConcurrentMap<ApplierKey, NettyClient> applierKeys = Maps.newConcurrentMap();

    private BinlogScannerManager binlogScannerManager;

    public ApplierRegisterCommandHandler(GtidManager gtidManager, FileManager fileManager, OutboundMonitorReport outboundMonitorReport, ReplicatorConfig replicatorConfig) {
        this.gtidManager = gtidManager;
        this.fileManager = fileManager;
        this.outboundMonitorReport = outboundMonitorReport;
        this.replicatorName = replicatorConfig.getRegistryKey();
        this.replicatorRegion = RegionConfig.getInstance().getRegion();
        this.binlogScannerManager = new DefaultBinlogScannerManager(this);
    }

    @Override
    public synchronized void handle(ServerCommandPacket serverCommandPacket, NettyClient nettyClient) {
        ApplierDumpCommandPacket dumpCommandPacket = (ApplierDumpCommandPacket) serverCommandPacket;
        logger.info("[Receive] command code is {}", COM_APPLIER_BINLOG_DUMP_GTID.name());
        String applierName = dumpCommandPacket.getApplierName();
        Channel channel = nettyClient.channel();
        String ip = getIpFromChannel(channel);
        ApplierKey applierKey = new ApplierKey(applierName, ip);
        if (applierKeys.containsKey(applierKey)) {
            logger.info("[Duplicate] request for applier {} and close channel {}", applierName, channel);
            channel.close();
            return;
        }

        try {
            binlogScannerManager.startSender(channel, dumpCommandPacket);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.applier.dump", applierName + ":" + ip);
            applierKeys.putIfAbsent(applierKey, nettyClient);
        } catch (Throwable e) {
            logger.error("[startSender] error for applier {} and close channel {}", applierName, channel, e);
            if (e instanceof ReplicatorException && channel.isActive()) {
                ReplicatorException replicatorException = (ReplicatorException) e;
                ResultCode resultCode = replicatorException.getResultCode();
                if (resultCode != null) {
                    resultCode.sendResultCode(channel, logger, e.getMessage());
                }
            }
            channel.close();
        }
    }

    public static String getIpFromChannel(Channel channel) {
        InetSocketAddress remoteAddress = (InetSocketAddress) channel.remoteAddress();
        return remoteAddress.getAddress().getHostAddress();
    }

    @Override
    public SERVER_COMMAND getCommandType() {
        return COM_APPLIER_BINLOG_DUMP_GTID;
    }

    @Override
    public void dispose() throws Exception {
        // todo by yongnian: 2024/4/11 repeat close, ok?
        for (Map.Entry<ApplierKey, NettyClient> applierKey : applierKeys.entrySet()) {
            try {
                applierKey.getValue().channel().close();
                logger.info("[NettyClient] close for {} in ApplierRegisterCommandHandler", applierKey.getKey());
            } catch (Exception e) {
                logger.error("applierKey close NettyClient error", e);
            }
        }
        binlogScannerManager.dispose();
    }

    public static class ApplierKey {

        private String applierName;

        private String ip;

        public ApplierKey(String applierName, String ip) {
            this.applierName = applierName;
            this.ip = ip;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ApplierKey that = (ApplierKey) o;
            return Objects.equals(applierName, that.applierName) &&
                    Objects.equals(ip, that.ip);
        }

        @Override
        public int hashCode() {

            return Objects.hash(applierName, ip);
        }

        @Override
        public String toString() {
            return "ApplierKey{" +
                    "applierName='" + applierName + '\'' +
                    ", ip='" + ip + '\'' +
                    '}';
        }
    }

    public GtidManager getGtidManager() {
        return gtidManager;
    }

    public FileManager getFileManager() {
        return fileManager;
    }

    public OutboundMonitorReport getOutboundMonitorReport() {
        return outboundMonitorReport;
    }

    public String getReplicatorRegion() {
        return replicatorRegion;
    }

    public String getReplicatorName() {
        return replicatorName;
    }

    public synchronized NettyClient removeApplierKeys(ApplierKey key) {
        return applierKeys.remove(key);
    }


    @VisibleForTesting
    public BinlogScannerManager getBinlogScannerManager() {
        return binlogScannerManager;
    }
}
