package com.ctrip.framework.drc.replicator.impl.oubound.handler;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.config.RegionConfig;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.common.SizeNotEnoughException;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.core.server.observer.gtid.GtidObserver;
import com.ctrip.framework.drc.core.server.utils.FileUtil;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.BinlogFileRegion;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundFilterChainContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundFilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager;
import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.utils.Gate;
import com.ctrip.framework.drc.core.utils.OffsetNotifier;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import static com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND.COM_APPLIER_BINLOG_DUMP_GTID;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.*;
import static com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager.LOG_EVENT_START;
import static com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager.LOG_FILE_PREFIX;


/**
 * deal with ApplierDumpCommandPacket
 * Created by mingdongli
 * 2019/9/21 10:13
 */
public class ApplierRegisterCommandHandler extends AbstractServerCommandHandler implements CommandHandler {

    private GtidManager gtidManager;

    private FileManager fileManager;

    private OutboundMonitorReport outboundMonitorReport;

    private ExecutorService dumpExecutorService;

    private boolean setGitdMode;

    private String replicatorRegion;

    private String replicatorName;

    private ConcurrentMap<ApplierKey, NettyClient> applierKeys = Maps.newConcurrentMap();

    public ApplierRegisterCommandHandler(GtidManager gtidManager, FileManager fileManager, OutboundMonitorReport outboundMonitorReport, ReplicatorConfig replicatorConfig) {
        this.gtidManager = gtidManager;
        this.fileManager = fileManager;
        this.outboundMonitorReport = outboundMonitorReport;
        this.replicatorName = replicatorConfig.getRegistryKey();
        this.dumpExecutorService = ThreadUtils.newCachedThreadPool(ThreadUtils.getThreadName("ARCH", replicatorConfig.getRegistryKey()));
        this.setGitdMode = replicatorConfig.getApplyMode() == ApplyMode.set_gtid.getType();
        this.replicatorRegion = RegionConfig.getInstance().getRegion();
    }

    @Override
    public synchronized void handle(ServerCommandPacket serverCommandPacket, NettyClient nettyClient) {
        ApplierDumpCommandPacket dumpCommandPacket = (ApplierDumpCommandPacket) serverCommandPacket;
        logger.info("[Receive] command code is {}", COM_APPLIER_BINLOG_DUMP_GTID.name());
        String applierName = dumpCommandPacket.getApplierName();
        Channel channel = nettyClient.channel();
        InetSocketAddress remoteAddress = (InetSocketAddress) channel.remoteAddress();
        String ip = remoteAddress.getAddress().getHostAddress();
        ApplierKey applierKey = new ApplierKey(applierName, ip);
        if (!applierKeys.containsKey(applierKey)) {
            try {
                DumpTask dumpTask = new DumpTask(nettyClient.channel(), dumpCommandPacket, ip);
                dumpExecutorService.submit(dumpTask);
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.applier.dump", applierName + ":" + ip);
                applierKeys.putIfAbsent(applierKey, nettyClient);
            } catch (Exception e) {
                logger.info("[DumpTask] error for applier {} and close channel {}", applierName, channel, e);
                channel.close();
            }
        } else {
            logger.info("[Duplicate] request for applier {} and close channel {}", applierName, channel);
            channel.close();
        }
    }

    @Override
    public SERVER_COMMAND getCommandType() {
        return COM_APPLIER_BINLOG_DUMP_GTID;
    }

    @Override
    public void dispose() {
        for (Map.Entry<ApplierKey, NettyClient> applierKey : applierKeys.entrySet()) {
            try {
                applierKey.getValue().channel().close();
                logger.info("[NettyClient] close for {} in ApplierRegisterCommandHandler", applierKey.getKey());
            } catch (Exception e) {
                logger.error("applierKey close NettyClient error", e);
            }
        }
        dumpExecutorService.shutdown();
    }

    private static class ApplierKey {

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

    class DumpTask implements Runnable, GtidObserver {

        private Gate gate;

        private Channel channel;

        private ApplierDumpCommandPacket dumpCommandPacket;

        private String applierName;

        private String nameFilter = null;

        private AviatorRegexFilter aviatorFilter = null;

        private String applierRegion;

        private String ip;

        private OffsetNotifier offsetNotifier = new OffsetNotifier(LOG_EVENT_START);

        private volatile long waitEndPosition;

        private volatile boolean channelClosed = false;

        private ResultCode resultCode;

        private ConsumeType consumeType;

        private String consumeName;

        private boolean skipDrcGtidLogEvent;

        private ChannelAttributeKey channelAttributeKey;

        private Filter<OutboundLogEventContext> filterChain;

        private OutboundLogEventContext outboundContext = new OutboundLogEventContext();

        public DumpTask(Channel channel, ApplierDumpCommandPacket dumpCommandPacket, String ip) throws Exception {
            this.channel = channel;
            this.dumpCommandPacket = dumpCommandPacket;
            this.applierName = dumpCommandPacket.getApplierName();
            this.consumeType = ConsumeType.getType(dumpCommandPacket.getConsumeType());
            this.consumeName = ConsumeType.Replicator == consumeType ? (replicatorName + "-slave") : applierName;
            this.skipDrcGtidLogEvent = setGitdMode && !consumeType.requestAllBinlog();
            String properties = dumpCommandPacket.getProperties();
            DataMediaConfig dataMediaConfig = DataMediaConfig.from(applierName, properties);
            this.applierRegion = dumpCommandPacket.getRegion();
            this.ip = ip;
            logger.info("[ConsumeType] is {}, [properties] is {}, [replicatorRegion] is {}, [applierRegion] is {}, for {} from {}", consumeType.name(), properties, replicatorRegion, applierRegion, applierName, ip);
            channelAttributeKey = channel.attr(ReplicatorMasterHandler.KEY_CLIENT).get();
            if (!consumeType.shouldHeartBeat()) {
                channelAttributeKey.setHeartBeat(false);
                HEARTBEAT_LOGGER.info("[HeartBeat] stop due to replicator slave for {}:{}", applierName, channel.remoteAddress().toString());
            }
            this.gate = channelAttributeKey.getGate();

            String filter = dumpCommandPacket.getNameFilter();
            logger.info("[Filter] before init name filter, applier name is: {}, filter is: {}", applierName, filter);
            if (StringUtils.isNotBlank(filter)) {
                this.nameFilter = filter;
                this.aviatorFilter = new AviatorRegexFilter(filter);
                logger.info("[Filter] init name filter, applier name is: {}, filter is: {}", applierName, filter);
            }

            filterChain = new OutboundFilterChainFactory().createFilterChain(
                    OutboundFilterChainContext.from(
                            this.applierName,
                            this.channel,
                            this.consumeType,
                            dataMediaConfig,
                            outboundMonitorReport,
                            dumpCommandPacket.getGtidSet(),
                            skipDrcGtidLogEvent,
                            nameFilter,
                            aviatorFilter,
                            channelAttributeKey,
                            replicatorRegion,
                            applierRegion
                    )
            );
        }

        private boolean check(GtidSet excludedSet) {
            //1. check the gtid set associated with the uuid of mysql master node
            GtidSet executedGtids = gtidManager.getExecutedGtids();
            String currentUuid = gtidManager.getCurrentUuid();
            GtidSet masterGtidSet = excludedSet.filterGtid(Sets.newHashSet(currentUuid));
            boolean masterGtidSetCheck = masterGtidSet.isContainedWithin(executedGtids);
            logger.info("[GtidSet][{}][{}] check master gtidset result: {}, master gtidset: {}, executed gtidset: {}",
                    consumeName, consumeType, masterGtidSetCheck, masterGtidSet, executedGtids);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.gtidset.check.master.uuid",
                    consumeName + "-" + consumeType + ":" + masterGtidSetCheck);
            if (!masterGtidSetCheck) {
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.gtidset.check.result",
                        consumeName + "-" + consumeType + ":" + false);
                return false;
            }

            //2. check gtid set associated with the uuids of mysql slave nodes, the result just for logging
            Set<String> slaveUuids = Sets.newHashSet(gtidManager.getUuids());
            slaveUuids.remove(currentUuid);
            GtidSet slaveGtidSet = excludedSet.filterGtid(slaveUuids);
            boolean slaveGtidSetCheck = slaveGtidSet.isContainedWithin(executedGtids);
            logger.info("[GtidSet][{}][{}] check slave gtidset result: {}, slave gtidset: {}, executed gtidset: {}",
                    consumeName, consumeType, slaveGtidSetCheck, slaveGtidSet, executedGtids);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.gtidset.check.slave.uuid",
                    consumeName + "-" + consumeType + ":" + slaveGtidSetCheck);

            //3. check purged gtid set
            GtidSet purgedGtidSet = gtidManager.getPurgedGtids();
            boolean purgedGtidSetCheck = purgedGtidSet.isContainedWithin(excludedSet);
            logger.info("[GtidSet][{}][{}] check purged gtidset result: {}, purged gtidset: {}, excluded gtidset: {}",
                    consumeName, consumeType, purgedGtidSetCheck, purgedGtidSet, excludedSet);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.gtidset.check.purged",
                    consumeName + "-" + consumeType + ":" + purgedGtidSetCheck);

            if (DynamicConfig.getInstance().getPurgedGtidSetCheckSwitch()) {
                logger.info("[GtidSet][{}][{}] check purged gtidset switch on, result: {}", consumeName, consumeType, purgedGtidSetCheck);
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.gtidset.check.result", consumeName + "-" + consumeType + ":" + purgedGtidSetCheck);
                return purgedGtidSetCheck;
            } else {
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.gtidset.check.result", consumeName + "-" + consumeType + ":" + true);
                return true;
            }
        }

        private File getFirstFile(GtidSet excludedSet, boolean onlyLocalUuids) {
            return gtidManager.getFirstLogNotInGtidSet(excludedSet, onlyLocalUuids);
        }

        private void addListener() {
            this.channel.closeFuture().addListener((ChannelFutureListener) future -> {
                Throwable throwable = future.cause();
                if (throwable != null) {
                    logger.error("DumpTask closeFuture", throwable);
                }
                channelClosed = true;
                gate.open();
                removeListener();
                logger.info("closeFuture Listener invoke open gate {} and set channelClosed", gate);
            });
            fileManager.addObserver(this);
        }

        private void removeListener() {
            removeObserver(this);
            NettyClient nettyClient = applierKeys.remove(new ApplierKey(applierName, ip));
            if (nettyClient != null) {
                nettyClient.channel().close();
            }
        }

        private File blankUuidSets() {
            if (isIntegrityTest()) {
                  return fileManager.getFirstLogFile();
            } else {
                resultCode = ResultCode.APPLIER_GTID_ERROR;
                logger.warn("[GTID SET] is blank for {}", dumpCommandPacket.getApplierName());
                return null;
            }
        }

        private File calculateGtidSet(GtidSet excludedSet) {
            // 1、clone gtid
            GtidSet clonedExcludedSet = excludedSet.clone();
            GtidSet filteredExcludedSet = excludedSet.filterGtid(gtidManager.getUuids());
            logger.info("[GtidSet] filter : excludedSet {}, filteredExcludedSet {}", excludedSet, filteredExcludedSet);

            // 2、check gtid
            if (!check(clonedExcludedSet)) {
                logger.warn("[GTID SET] {} not valid for {}", dumpCommandPacket.getGtidSet(), applierName);
                resultCode = ResultCode.APPLIER_GTID_ERROR;
                return null;
            }

            // 3、find first file
            return consumeType.isSlave() ? getFirstFile(clonedExcludedSet, !consumeType.isSlave()) : getFirstFile(filteredExcludedSet, !consumeType.isSlave());
        }

        private File firstFileToSend() {
            GtidSet excludedSet = dumpCommandPacket.getGtidSet();
            Collection<GtidSet.UUIDSet> uuidSets = excludedSet.getUUIDSets();
            return (uuidSets == null || uuidSets.isEmpty()) ? blankUuidSets() : calculateGtidSet(excludedSet);
        }

        private void sendResultCode() {
            if (resultCode == null) {
                logger.warn("[Replicator] not ready to serve {}", applierName);
                resultCode = ResultCode.REPLICATOR_NOT_READY;
            }
            resultCode.sendResultCode(channel, logger);
        }

        @Override
        public void run() {
            try {
                GtidSet excludedSet = dumpCommandPacket.getGtidSet();
                addListener();
                File file = firstFileToSend();
                if (file == null) {
                    sendResultCode();
                    return;
                }

                checkFileGaps(file);

                logger.info("[Serving] {} begin, first file name {}", applierName, file.getName());
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.serve.binlog", applierName);
                // 3、open file，send every file
                while (loop()) {
                    if (sendBinlog(file) == 1) {
                        if (channelClosed) {
                            logger.info("[Inactive] for {}", applierName);
                            return;
                        }
                        ResultCode.REPLICATOR_SEND_BINLOG_ERROR.sendResultCode(channel, logger);
                        logger.info("[Send] binlog error for {}", applierName);
                        return;
                    }

                    // 4、get next file
                    do {
                        String previousFileName = file.getName();
                        file = fileManager.getNextLogFile(file);
                        String currentFileName = file.getName();
                        logger.info("[Transfer] binlog file from {} to {} for {}", previousFileName, currentFileName, applierName);
                    } while (fileManager.gtidExecuted(file, excludedSet));
                }
                logger.info("{} exit loop with channelClosed {}", applierName, channelClosed);
            } catch (Throwable e) {
                logger.error("dump thread error and close channel {}", channel.remoteAddress().toString(), e);
                channel.close();
            } finally {
                filterChain.release();
            }
        }

        private void checkFileGaps(File file) {
            try {
                File currentFile = fileManager.getCurrentLogFile();
                long firstSendFileNum = FileUtil.getFileNumFromName(file.getName(), LOG_FILE_PREFIX);
                long currentFileNum = FileUtil.getFileNumFromName(currentFile.getName(), LOG_FILE_PREFIX);
                if (currentFileNum - firstSendFileNum > 10) {
                    DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.applier.gap", applierName + ":" + ip);
                }
            } catch (Exception e) {
                logger.info("checkFileHasGaps error for {}", applierName, e);
            }
        }

        private void removeObserver(DumpTask dumpTask) {
            fileManager.removeObserver(dumpTask);
            logger.info("[Remove] observer of DumpTask {}:{} from fileManager", applierName, ip);
        }

        private boolean sendEvents(FileChannel fileChannel, long position, long endPos) throws Exception {
            outboundContext.setFileChannel(fileChannel);
            while (endPos > position && !channelClosed) {  //read event in while
                gate.tryPass();
                if (channelClosed) {
                    logger.info("channelClosed and return sendEvents");
                    return false;
                }

                outboundContext.reset(position, endPos);

                filterChain.doFilter(outboundContext);

                position = fileChannel.position();
                endPos = fileChannel.size();

                Exception sendException = outboundContext.getCause();
                if (sendException != null) {
                    if (sendException instanceof SizeNotEnoughException) {
                        continue;
                    } else {
                        throw sendException;
                    }
                }
            }

            return true;
        }

        /**
         * 0 mean reaching the end, 1 mean fail, otherwise endPos
         *
         * @param fileChannel
         * @param file
         * @return
         * @throws IOException
         */
        private long getBinlogEndPos(FileChannel fileChannel, File file, long logPos) throws IOException {
            String fileName = file.getName();
            long endPos = fileChannel.size();
            do {
                String currentFileName = fileManager.getCurrentLogFileName();
                if (!fileName.equals(currentFileName)) {  //file rolled
                    endPos = fileChannel.size();
                    if (logPos == endPos) {  //read to the tail
                        logger.info("[Reaching] {} end position and write empty msg to close fileChannel", fileName);
                        channel.writeAndFlush(new BinlogFileRegion(fileChannel, endPos, 0, applierName, fileName));
                        return 0;
                    } else {
                        return endPos;
                    }
                }

                if (logPos < endPos) {
                    return endPos;
                }

                endPos = waitNewEvents(logPos); //== to wait
                if (endPos <= 0) {
                    return 1;
                }
            } while (loop());

            return 1;
        }

        private long waitNewEvents(long endPos) {
            waitEndPosition = endPos + 1;
            long acquiredOffset = -1;
            do {
                try {
                    acquiredOffset = offsetNotifier.await(waitEndPosition, 500);
                    if (acquiredOffset > 0) {
                        logger.debug("offsetNotifier acquired for {}", waitEndPosition);
                        return acquiredOffset;
                    }
                } catch (InterruptedException e) {
                    logger.error("[Read] error", e);
                    Thread.currentThread().interrupt();
                }
            } while (loop());
            return acquiredOffset;
        }

        private long sendBinlog(File file) throws Exception {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            FileChannel fileChannel = raf.getChannel();
            if (fileChannel.position() == 0) {
                fileChannel.position(DefaultFileManager.LOG_EVENT_START);
            }

            while (loop()) {
                long position = fileChannel.position();
                long endPosition = getBinlogEndPos(fileChannel, file, position);
                if (endPosition <= 1) {
                    return endPosition;
                }
                if (!sendEvents(fileChannel, position, endPosition)) {
                    return 1;
                }
            }
            return 1;
        }

        private boolean loop() {
            return !Thread.currentThread().isInterrupted() && !channelClosed;
        }

        @Override
        public void update(Object args, Observable observable) {
            long position = (Long) args;
            if (logger.isDebugEnabled()) {
                logger.debug("[OffsetNotifier] update position {}, waitEndPosition {}", position, waitEndPosition);
            }
            if (position < waitEndPosition) {  //file rolled
                offsetNotifier.offsetIncreased(waitEndPosition + position);
            } else {
                offsetNotifier.offsetIncreased(position);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DumpTask dumpTask = (DumpTask) o;
            return Objects.equals(channel, dumpTask.channel) &&
                    Objects.equals(dumpCommandPacket, dumpTask.dumpCommandPacket);
        }

        @Override
        public int hashCode() {

            return Objects.hash(channel, dumpCommandPacket);
        }
    }
}
