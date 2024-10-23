package com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.observer.gtid.GtidObserver;
import com.ctrip.framework.drc.core.utils.OffsetNotifier;
import com.ctrip.framework.drc.replicator.impl.oubound.ReplicatorException;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.AbstractBinlogScanner;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.AbstractBinlogScannerManager;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogScanner;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogSender;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.BinlogFileRegion;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.scanner.ScannerFilterChainContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.scanner.ScannerFilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.ApplierRegisterCommandHandler;
import com.ctrip.framework.drc.replicator.store.manager.file.BinlogPosition;
import com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager;
import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Sets;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.BINLOG_SCANNER_LOGGER;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.isIntegrityTest;
import static com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager.LOG_EVENT_START;

/**
 * @author yongnian
 */
public class DefaultBinlogScanner extends AbstractBinlogScanner implements GtidObserver {
    private final ApplierRegisterCommandHandler applierRegisterCommandHandler;
    private final GtidManager gtidManager;
    private final FileManager fileManager;
    private File file;
    private long fileNum;
    private Filter<OutboundLogEventContext> filterChain;

    private static final AtomicInteger atomicInteger = new AtomicInteger(0);

    public DefaultBinlogScanner(ApplierRegisterCommandHandler applierRegisterCommandHandler, AbstractBinlogScannerManager manager, List<BinlogSender> binlogSenders) {
        super(manager, binlogSenders);
        this.applierRegisterCommandHandler = Objects.requireNonNull(applierRegisterCommandHandler);
        this.consumeName = String.format("%s-%s", applierRegisterCommandHandler.getReplicatorName(), ConsumeType.Replicator == consumeType ? "slave" : atomicInteger.getAndIncrement());
        this.gtidManager = Objects.requireNonNull(applierRegisterCommandHandler.getGtidManager());
        this.fileManager = Objects.requireNonNull(applierRegisterCommandHandler.getFileManager());
    }

    @Override
    public String getCurrentSendingFileName() {
        if (file != null) {
            return file.getName();
        }
        return null;
    }

    @Override
    public BinlogPosition getBinlogPosition() {
        return BinlogPosition.from(fileNum, outboundContext.getFileChannelPos());
    }

    @Override
    public BinlogScanner cloneScanner(List<BinlogSender> senders) {
        try {
            DefaultBinlogScanner newScanner = new DefaultBinlogScanner(this.applierRegisterCommandHandler, this.manager, senders);
            newScanner.setFile(file);
            newScanner.initialize();
            newScanner.setFileChannel(newScanner.outboundContext);
            newScanner.outboundContext.getFileChannel().position(this.outboundContext.getFileChannelPos());
            newScanner.outboundContext.setFileChannelPos(this.outboundContext.getFileChannelPos());
            newScanner.outboundContext.setInSchemaExcludeGroup(this.outboundContext.isInSchemaExcludeGroup());
            newScanner.outboundContext.setInGtidExcludeGroup(this.outboundContext.isInGtidExcludeGroup());
            return newScanner;
        } catch (Exception e) {
            BINLOG_SCANNER_LOGGER.error("unlikely: cloneScanner error. ", e);
            return null;
        }
    }

    @Override
    protected void doInitialize() throws Exception {
        this.rebuildFilterChain();
        if (file == null) {
            this.setFile(this.firstFileToSend());
        }
        if (this.file == null) {
            throw new ReplicatorException(ResultCode.REPLICATOR_NOT_READY);
        }
        this.checkFileGaps(this.file);
        logger.info("[Serving] {} begin, first file name {}", consumeName, this.file.getName());
        for (BinlogSender sender : getSenders()) {
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.serve.binlog", sender.getApplierName());
        }
        fileManager.addObserver(this);
    }

    private void setFile(File file) {
        this.file = Objects.requireNonNull(file);
        this.fileNum = DefaultFileManager.getFileNum(file);
    }

    @Override
    protected Set<String> getUUids() {
        return gtidManager.getUuids();
    }

    @Override
    public void doDispose() throws Exception {
        super.doDispose();
        fileManager.removeObserver(this);
        if (filterChain != null) {
            filterChain.release();
        }
    }


    public static void sendResult(ResultCode resultCode, List<BinlogSender> senders) {
        for (BinlogSender sender : senders) {
            sender.send(resultCode);
        }
    }

    private void rebuildFilterChain() {
        if (filterChain != null) {
            filterChain.release();
        }
        filterChain = new ScannerFilterChainFactory().createFilterChain(
                ScannerFilterChainContext.from(
                        consumeName,
                        consumeType,
                        excludedSet,
                        this
                )
        );
    }


    @Override
    protected void setFileChannel(OutboundLogEventContext context) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        FileChannel fileChannel = raf.getChannel();
        if (fileChannel.position() == 0) {
            fileChannel.position(DefaultFileManager.LOG_EVENT_START);
        }
        context.setFileChannel(fileChannel);
        context.setFileSeq(fileNum);
        context.setFileChannelPos(fileChannel.position());
    }

    @Override
    protected void readFilePosition(OutboundLogEventContext context) throws IOException {
        FileChannel fileChannel = context.getFileChannel();
        long position = fileChannel.position();
        long endPosition = getBinlogEndPos(fileChannel, position);
        context.reset(position, endPosition);
    }

    @Override
    public void readNextEvent(OutboundLogEventContext context) {
        filterChain.doFilter(context);
    }

    @Override
    public void fileRoll() {
        do {
            String previousFileName = file.getName();
            this.setFile(fileManager.getNextLogFile(file));
            recordGtidSent(file);
            String currentFileName = file.getName();
            logger.info("[Transfer] binlog file from {} to {} for {}", previousFileName, currentFileName, consumeName);
        } while (fileManager.gtidExecuted(file, excludedSet));
    }

    private void recordGtidSent(File nextFile) {
        GtidSet nextFilepreviousGtids = fileManager.getPreviousGtids(nextFile);
        excludedSet.unionInPlace(nextFilepreviousGtids);
        logger.info("[GtidSet] {} has been sent for {}", nextFilepreviousGtids, consumeName);
    }

    /**
     * @see DefaultBinlogScanner#setFileChannel(OutboundLogEventContext)
     */
    protected long getBinlogEndPos(FileChannel fileChannel, long logPos) throws IOException {
        String fileName = file.getName();
        long endPos = fileChannel.size();
        do {
            String currentFileName = fileManager.getCurrentLogFileName();
            if (!fileName.equals(currentFileName)) {  //file rolled
                endPos = fileChannel.size();
                if (logPos == endPos) {  //read to the tail
                    logger.info("[Reaching] {} end position and write empty msg to close fileChannel", fileName);
                    BinlogFileRegion binlogFileRegion = new BinlogFileRegion(fileChannel, endPos, 0, getName(), fileName);
                    if (senders.size() - 1 > 0) {
                        binlogFileRegion.retain(senders.size() - 1);
                    }
                    for (BinlogSender sender : senders) {
                        if (sender.isRunning()) {
                            sender.getChannel().writeAndFlush(binlogFileRegion);
                        } else {
                            binlogFileRegion.release(1);
                        }
                    }
                    return REACH_FILE_END_FLAG;
                } else {
                    return endPos;
                }
            }

            if (logPos < endPos) {
                return endPos;
            }

            endPos = waitNewEvents(fileChannel, logPos); //== to wait
        } while (loop());
        throw new ReplicatorException(ResultCode.SCANNER_STOP);
    }

    private volatile long waitEndPosition;
    private OffsetNotifier offsetNotifier = new OffsetNotifier(LOG_EVENT_START);

    private long waitNewEvents(FileChannel fileChannel, long endPos) throws IOException {
        waitEndPosition = endPos + 1;
        int waitCount = 0;
        do {
            try {
                long acquiredOffset = offsetNotifier.await(waitEndPosition, 100);
                if (acquiredOffset > 0) {
                    return acquiredOffset;
                }
                if (++waitCount > 25) {
                    return fileChannel.size();
                }
                manager.tryMergeScanner(this);
            } catch (InterruptedException e) {
                logger.error("[Read] error", e);
                Thread.currentThread().interrupt();
            }
        } while (loop());
        throw new ReplicatorException(ResultCode.SCANNER_STOP);
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

    private File firstFileToSend() throws ReplicatorException {
        if (isIntegrityTest()) {
            return fileManager.getFirstLogFile();
        }
        Collection<GtidSet.UUIDSet> uuidSets = excludedSet.getUUIDSets();
        if (CollectionUtils.isEmpty(uuidSets)) {
            return getByBlankUuidSets();
        }
        return calculateGtidSet(excludedSet);
    }


    private File getByBlankUuidSets() throws ReplicatorException {
        if (isIntegrityTest()) {
            return fileManager.getFirstLogFile();
        }
        throw new ReplicatorException(ResultCode.APPLIER_GTID_ERROR);
    }


    private File calculateGtidSet(GtidSet excludedSet) throws ReplicatorException {
        // 1、clone gtid
        GtidSet clonedExcludedSet = excludedSet.clone();
        GtidSet filteredExcludedSet = excludedSet.filterGtid(gtidManager.getUuids());
        logger.info("[GtidSet] filter : excludedSet {}, filteredExcludedSet {}", excludedSet, filteredExcludedSet);

        // 2、check gtid
        check(clonedExcludedSet);

        // 3、find first file
        if (consumeType.isSlave()) {
            return getFirstFile(clonedExcludedSet, false);
        }
        return getFirstFile(filteredExcludedSet, true);
    }

    private File getFirstFile(GtidSet excludedSet, boolean onlyLocalUuids) {
        File firstLogNotInGtidSet = gtidManager.getFirstLogNotInGtidSet(excludedSet, onlyLocalUuids);
        if (firstLogNotInGtidSet != null) {
            return firstLogNotInGtidSet;
        } else {
            GtidSet purgedGtids = gtidManager.getPurgedGtids();
            if (onlyLocalUuids) {
                purgedGtids = purgedGtids.filterGtid(gtidManager.getUuids());
            }
            GtidSet gtidSet = purgedGtids.subtract(excludedSet);
            // purged
            if (gtidSet != null && gtidSet.getGtidNum() > 0) {
                throw new ReplicatorException(ResultCode.REPLICATOR_BINLOG_PURGED, "gtidSet purged: " + gtidSet);
            }
            GtidSet executedGtids = gtidManager.getExecutedGtids();
            if (onlyLocalUuids) {
                executedGtids = executedGtids.filterGtid(gtidManager.getUuids());
            }
            // not ready
            if (executedGtids.isContainedWithin(excludedSet)) {
                throw new ReplicatorException(ResultCode.REPLICATOR_NOT_READY, "gtidSet gap: " + excludedSet.subtract(executedGtids));
            }
            throw new ReplicatorException(ResultCode.REPLICATOR_NOT_READY);
        }
    }

    private void check(GtidSet excludedSet) {
        //1. check the gtid set associated with the uuid of mysql master node
        GtidSet executedGtids = gtidManager.getExecutedGtids();
        String currentUuid = gtidManager.getCurrentUuid();
        GtidSet masterGtidSet = excludedSet.filterGtid(Sets.newHashSet(currentUuid));
        boolean masterGtidSetCheck = masterGtidSet.isContainedWithin(executedGtids);
        if (!masterGtidSetCheck) {
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.gtidset.check.result",
                    consumeName + "-" + consumeType + ":" + false);
            throw new ReplicatorException(ResultCode.APPLIER_GTID_ERROR);
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
            if (!purgedGtidSetCheck) {
                throw new ReplicatorException(ResultCode.APPLIER_GTID_ERROR);
            }
        } else {
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.gtidset.check.result", consumeName + "-" + consumeType + ":" + true);
        }
    }

    private void checkFileGaps(File file) {
        try {
            File currentFile = fileManager.getCurrentLogFile();
            long firstSendFileNum = DefaultFileManager.getFileNum(file);
            long currentFileNum = DefaultFileManager.getFileNum(currentFile);
            if (currentFileNum - firstSendFileNum > 10) {
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.applier.gap", consumeName);
            }
        } catch (Exception e) {
            logger.info("checkFileHasGaps error for {}", consumeName, e);
        }
    }

    @Override
    protected boolean isConcern(OutboundLogEventContext context) {
        return !context.isSkipEvent();
    }

    @VisibleForTesting
    public Filter<OutboundLogEventContext> getFilterChain() {
        return filterChain;
    }
}
