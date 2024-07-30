package com.ctrip.framework.drc.replicator.impl.oubound.binlog;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.common.SizeNotEnoughException;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.replicator.impl.oubound.ReplicatorException;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.BINLOG_SCANNER_LOGGER;

/**
 * @author yongnian
 */
public abstract class AbstractBinlogScanner extends AbstractLifecycle implements BinlogScanner {
    public static final int REACH_FILE_END_FLAG = 0;

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final AbstractBinlogScannerManager manager;
    protected List<BinlogSender> senders;
    protected final ConsumeType consumeType;
    protected final GtidSet excludedSet;
    protected String consumeName;
    protected final OutboundLogEventContext outboundContext = new OutboundLogEventContext();

    @Override
    public GtidSet getFilteredGtidSet() {
        return excludedSet.filterGtid(getUUids());
    }

    public AbstractBinlogScanner(AbstractBinlogScannerManager manager, List<BinlogSender> binlogSenders) {
        validateSenders(binlogSenders);
        this.manager = manager;
        this.senders = new CopyOnWriteArrayList<>();
        this.senders.addAll(binlogSenders);
        List<GtidSet> sendersGtid = senders.stream().map(BinlogSender::getGtidSet).collect(Collectors.toList());
        this.excludedSet = GtidSet.getIntersection(sendersGtid);
        this.consumeType = getConsumeType(binlogSenders);
    }

    private static void validateSenders(List<BinlogSender> binlogSenders) {
        if (CollectionUtils.isEmpty(binlogSenders)) {
            throw new IllegalArgumentException("empty binlogSenders");
        }
        for (BinlogSender binlogSender : binlogSenders) {
            Objects.requireNonNull(binlogSender);
            Objects.requireNonNull(binlogSender.getConsumeType());
        }
    }

    @Override
    public boolean canMerge() {
        return consumeType != ConsumeType.Replicator && this.senders.size() < DynamicConfig.getInstance().getMaxSenderNumPerScanner();
    }

    private ConsumeType getConsumeType(List<BinlogSender> binlogSenders) {
        List<ConsumeType> list = binlogSenders.stream().map(BinlogSender::getConsumeType).distinct().collect(Collectors.toList());
        if (list.size() > 1) {
            throw new IllegalArgumentException("only serve for same consumeType");
        }
        return list.get(0);
    }


    @Override
    public synchronized void addSenders(BinlogScanner another) {
        if (another == this) {
            // cannot merge itself
            return;
        }
        this.senders.addAll(another.getSenders());
    }

    @Override
    public String getCurrentSendingFileName() {
        return "unknown";
    }

    @Override
    public void splitConcernSenders(String schema) {
        List<BinlogSender> sendersToSplit = senders.stream().filter(e -> e.concernSchema(schema)).collect(Collectors.toList());
        if (this.canNotSplit(sendersToSplit)) {
            return;
        }
        DefaultTransactionMonitorHolder.getInstance().logTransactionSwallowException("drc.replicator.scanner.split.concern", getName(), () -> {
            BINLOG_SCANNER_LOGGER.info("[splitSenders] splitConcernSenders: {} from {} ", sendersToSplit, senders);
            this.splitSenders(sendersToSplit);
        });
    }

    public abstract BinlogScanner cloneScanner(List<BinlogSender> senders);

    protected Set<String> getUUids() {
        return excludedSet.getUUIDs();
    }

    @Override
    public GtidSet getGtidSet() {
        return excludedSet;
    }

    @Override
    public List<BinlogSender> getSenders() {
        return Collections.unmodifiableList(senders);
    }

    @Override
    public void run() {
        try {
            if (!getLifecycleState().isInitialized()) {
                logger.warn("scanner initialized fail. stop running");
                return;
            }
            this.start();
            if (outboundContext.getFileChannelPos() == 0) {
                this.setFileChannel(outboundContext);
            }
            while (loop()) {
                this.readFilePosition(outboundContext);
                if (outboundContext.getFileChannelSize() == REACH_FILE_END_FLAG) {
                    this.fileRoll();
                    this.setFileChannel(outboundContext);
                    continue;
                }
                this.readNextEvent(outboundContext);
                if (!this.checkException(outboundContext)) {
                    continue;
                }
                this.notifySenders(outboundContext);
            }
        } catch (ReplicatorException e) {
            ResultCode resultCode = e.getResultCode();
            if (resultCode == ResultCode.SCANNER_STOP) {
                return;
            }
            logger.error("scanner run exception.", e);
            this.sendResult(resultCode);
            OutboundLogEventContext context = e.getContext();
            if (context != null) {
                logger.error("context: {}", context);
            }
        } catch (Throwable e) {
            logger.error("scanner run unknown error.", e);
        } finally {
            manager.removeScanner(this, true);
        }
    }

    private void sendResult(ResultCode resultCode) {
        for (BinlogSender sender : senders) {
            sender.send(resultCode);
        }
    }


    protected boolean checkException(OutboundLogEventContext context) throws Exception {
        Exception sendException = context.getCause();
        if (sendException != null) {
            if (sendException instanceof SizeNotEnoughException) {
                return false;
            } else {
                throw sendException;
            }
        }
        return true;
    }

    @Override
    public void doDispose() throws Exception {
        // clean sender
        senders.clear();
        // close current file channel
        FileChannel fileChannel = outboundContext.getFileChannel();
        if (fileChannel != null) {
            fileChannel.close();
        }
    }

    protected boolean loop() {
        this.senders.removeIf(sender -> !sender.isRunning());
        return !Thread.currentThread().isInterrupted() && !senders.isEmpty();
    }

    @Override
    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public void notifySenders(OutboundLogEventContext context) throws Exception {
        if (!this.isConcern(context)) {
            return;
        }
        this.preSend(context);
        for (BinlogSender sender : senders) {
            sender.send(context);
        }
        this.postSend(context);
    }

    protected void preSend(OutboundLogEventContext context) {
        if (isTransactionEnd(context)) {
            splitSlowSenders();
        }
    }

    private void splitSlowSenders() {
        List<BinlogSender> blockedSenders = senders.stream()
                .filter(e -> e.getChannelAttributeKey().getGate().closeIfScheduled())
                .collect(Collectors.toList());
        if (canNotSplit(blockedSenders)) {
            return;
        }
        // splitConcernSenders
        DefaultTransactionMonitorHolder.getInstance().logTransactionSwallowException("drc.replicator.scanner.split.slow", getName(), () -> {
            BINLOG_SCANNER_LOGGER.info("[splitSenders] splitSlowSenders: {} from {}", blockedSenders, senders);
            // real close
            this.splitSenders(blockedSenders);
        });
    }


    private boolean canNotSplit(List<BinlogSender> sendersToSplit) {
        return CollectionUtils.isEmpty(sendersToSplit) || sendersToSplit.size() >= senders.size();
    }


    private void splitSenders(List<BinlogSender> slowSenders) {
        BinlogScanner binlogScanner = this.cloneScanner(slowSenders);
        if (binlogScanner != null) {
            manager.startScanner(binlogScanner);
            senders.removeAll(slowSenders);
        }
    }

    private void postSend(OutboundLogEventContext context) {
        if (isTransactionEnd(context)) {
            tryMerge();
        }
    }

    private void tryMerge() {
        if (this.canMerge()) {
            manager.tryMergeScanner(this);
        }
    }

    private boolean isTransactionEnd(OutboundLogEventContext value) {
        return value.getEventType() == LogEventType.xid_log_event;
    }

    protected abstract boolean isConcern(OutboundLogEventContext context);

    protected abstract void readNextEvent(OutboundLogEventContext context);

    protected abstract void setFileChannel(OutboundLogEventContext context) throws IOException;

    protected abstract void fileRoll();

    protected abstract void readFilePosition(OutboundLogEventContext context) throws IOException;


    @Override
    public int compareTo(BinlogScanner another) {
        return Integer.compare(this.getSenders().size(), another.getSenders().size());
    }

    @Override
    public String toString() {
        return String.format("[consumeName]: %s, [senders]: %s, [excludedGtid]: %s.", consumeName, senders, excludedSet);
    }

    @Override
    public String getName() {
        return consumeName;
    }
}
