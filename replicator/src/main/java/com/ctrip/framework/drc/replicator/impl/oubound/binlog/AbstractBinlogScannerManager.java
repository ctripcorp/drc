package com.ctrip.framework.drc.replicator.impl.oubound.binlog;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.replicator.impl.oubound.ReplicatorException;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.google.common.collect.Lists;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.BINLOG_SCANNER_LOGGER;

/**
 * @author yongnian
 */
public abstract class AbstractBinlogScannerManager implements BinlogScannerManager {
    public static final int COLLECT_MILLISECONDS = 100;
    public static final String MEASUREMENT = "fx.drc.replicator.scanner";
    private static final List<ConsumeType> MERGE_TYPE = Lists.newArrayList(ConsumeType.Applier, ConsumeType.Messenger);
    protected final Logger logger = BINLOG_SCANNER_LOGGER;
    // must be thread-safe
    protected final Map<ConsumeType, List<BinlogScanner>> scannerMap = new ConcurrentHashMap<>();
    protected final List<BinlogScanner> waitMergeScanner = Lists.newArrayList();
    protected final String registryKey;
    protected final String mhaName;
    private final ScheduledExecutorService mergeService;
    private final ExecutorService executorService;
    private volatile ConsumeType collectingType = null;
    private volatile boolean running = true;
    private final Map<String, String> tags;

    protected abstract AbstractBinlogScanner createScannerInner(List<BinlogSender> binlogSenders) throws Exception;

    protected abstract BinlogSender createSenderInner(Channel channel, ApplierDumpCommandPacket dumpCommandPacket) throws Exception;

    public AbstractBinlogScannerManager(String registryKey) {
        this.registryKey = registryKey;
        this.mhaName = RegistryKey.from(registryKey).getMhaName();
        this.tags = new HashMap<>();
        tags.put("mha", mhaName);
        executorService = ThreadUtils.newCachedThreadPool(String.format("BS-%s", registryKey));
        mergeService = ThreadUtils.newSingleThreadScheduledExecutor(String.format("SM-%s", registryKey));
        mergeService.scheduleWithFixedDelay(this::loop, 0, 1000, TimeUnit.MILLISECONDS);
    }


    @Override
    public synchronized BinlogSender startSender(Channel channel, ApplierDumpCommandPacket dumpCommandPacket) throws Exception {
        ConsumeType consumeType = ConsumeType.getType(dumpCommandPacket.getConsumeType());
        List<BinlogScanner> scannerList = scannerMap.get(consumeType);
        if (scannerList != null && scannerList.size() >= DynamicConfig.getInstance().getMaxScannerNumPerMha()) {
            throw new ReplicatorException(ResultCode.REPLICATOR_FULL_USE);
        }
        BinlogSender sender = this.createSenderInner(channel, dumpCommandPacket);
        this.createScanner(Lists.newArrayList(sender));
        return sender;
    }

    /**
     * @param binlogSenders should be started
     */
    @Override
    public synchronized void createScanner(List<BinlogSender> binlogSenders) throws Exception {
        AbstractBinlogScanner newScanner = this.createScannerInner(binlogSenders);
        List<BinlogScanner> scannerList = scannerMap.computeIfAbsent(newScanner.getConsumeType(), k -> new CopyOnWriteArrayList<>());
        scannerList.add(newScanner);
        try {
            newScanner.initialize();
        } catch (Throwable e) {
            scannerList.remove(newScanner);
            throw e;
        } finally {
            Collections.sort(scannerList);
        }
        executorService.submit(newScanner);
        BINLOG_SCANNER_LOGGER.info("[createScanner] create sender {}, scanner {}", binlogSenders, newScanner);
    }


    @Override
    public void startScanner(BinlogScanner scanner) {
        List<BinlogScanner> scannerList = scannerMap.computeIfAbsent(scanner.getConsumeType(), k -> new CopyOnWriteArrayList<>());
        scannerList.add(scanner);
        Collections.sort(scannerList);
        executorService.submit(scanner);
        BINLOG_SCANNER_LOGGER.info("[startScanner] scanner {}", scanner);
    }

    @Override
    public void tryMergeScanner(BinlogScanner src) {
        ConsumeType consumeType = src.getConsumeType();
        if (!this.isCollecting(consumeType)) {
            return;
        }
        synchronized (waitMergeScanner) {
            if (!this.isCollecting(consumeType)) {
                return;
            }
            waitMergeScanner.add(src);
            try {
                while (waitMergeScanner.contains(src)) {
                    waitMergeScanner.wait(COLLECT_MILLISECONDS);
                }
            } catch (InterruptedException e) {
                logger.error(src + "merge interrupt", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean isCollecting(ConsumeType consumeType) {
        return collectingType != null && collectingType == consumeType;
    }

    static class MergeAlgorithm {
        BinlogScanner mergeTo;
        List<BinlogScanner> candidates;

        public MergeAlgorithm(BinlogScanner mergeTo, List<BinlogScanner> candidates) {
            this.mergeTo = mergeTo;
            this.candidates = candidates;
        }

        public static MergeAlgorithm calculate(List<BinlogScanner> scannersList, int maxGtidGap, int maxSenderNumPerScanner) {
            if (scannersList == null) {
                return stop();
            }
            scannersList = scannersList.stream().filter(BinlogScanner::canMerge).collect(Collectors.toList());
            while (!CollectionUtils.isEmpty(scannersList)) {
                // for each loop: find the slowest scanner, and candidates that can merge to it
                MergeAlgorithm mergeAlgorithm = calculateOnce(scannersList, maxGtidGap, maxSenderNumPerScanner);

                if (mergeAlgorithm.mergeTo == null) {
                    // no slowest scanner found, cannot merge
                    return stop();
                }
                if (CollectionUtils.isEmpty(mergeAlgorithm.candidates)) {
                    // no candidates for the slowest scanner, remove it and run next round
                    scannersList.remove(mergeAlgorithm.mergeTo);
                    continue;
                }

                return mergeAlgorithm;
            }
            return stop();
        }

        private static MergeAlgorithm stop() {
            return new MergeAlgorithm(null, null);
        }

        public static MergeAlgorithm calculateOnce(List<BinlogScanner> scannersList, int maxGtidGap, int maxSenderNumPerScanner) {
            scannersList = scannersList.stream().filter(BinlogScanner::canMerge).collect(Collectors.toList());
            // 1. get slowest scanner
            GtidSet slowestGtid = GtidSet.getIntersection(scannersList.stream().map(BinlogScanner::getFilteredGtidSet).collect(Collectors.toList()));
            List<BinlogScanner> slowScanners = scannersList.stream().sorted().filter(e -> e.getFilteredGtidSet().equals(slowestGtid)).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(slowScanners)) {
                return stop();
            }
            Collections.sort(slowScanners);
            // mergeTo: slowest scanner with max number of sender
            BinlogScanner mergeTo = slowScanners.get(slowScanners.size() - 1);
            scannersList = scannersList.stream().filter(e -> e != mergeTo).collect(Collectors.toList());

            // 2. filter
            scannersList = scannersList.stream()
                    .filter(e -> {
                        // check gtid gap
                        long gap = e.getFilteredGtidSet().subtract(slowestGtid).getGtidNum();
                        return gap <= maxGtidGap;
                    })
                    .filter(e -> {
                        // check sender num
                        return e.getSenders().size() <= mergeTo.getSenders().size();
                    })
                    .sorted(Comparator.comparing(e -> e.getFilteredGtidSet().subtract(slowestGtid).getGtidNum()))
                    .collect(Collectors.toList());

            // 3. collect
            List<BinlogScanner> candidates = Lists.newArrayList();
            int finalSize = mergeTo.getSenders().size();
            for (BinlogScanner scanner : scannersList) {
                if (scanner.getSenders().size() + finalSize <= maxSenderNumPerScanner) {
                    candidates.add(scanner);
                    finalSize += scanner.getSenders().size();
                }
            }
            return new MergeAlgorithm(mergeTo, candidates);
        }


        public boolean canMerge() {
            return mergeTo != null && !CollectionUtils.isEmpty(candidates);
        }

    }


    private long lastRefreshTime = System.currentTimeMillis();

    public void loop() {
        try {
            if (!running) {
                return;
            }
            if (System.currentTimeMillis() - lastRefreshTime < DynamicConfig.getInstance().getMergeCheckPeriodMilli()) {
                return;
            }
            merge();
            lastRefreshTime = System.currentTimeMillis();
        } catch (Throwable e) {
            BINLOG_SCANNER_LOGGER.error("merge exception.", e);
        }
    }

    private void merge() throws Exception {
        this.report();
        for (ConsumeType consumeType : MERGE_TYPE) {
            List<BinlogScanner> scanners = scannerMap.get(consumeType);
            // pre-check merge possibility, without actually block sending process
            if (!calculate(scanners).canMerge()) {
                return;
            }
            // start collect waitMergeScanner
            DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.replicator.scanner.merge", registryKey, () -> {
                collectingType = consumeType;
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(COLLECT_MILLISECONDS));
                collectingType = null;
                synchronized (waitMergeScanner) {
                    try {
                        while (!waitMergeScanner.isEmpty()) {
                            long start = System.currentTimeMillis();
                            MergeAlgorithm mergeAlgorithm = calculate(waitMergeScanner);
                            if (!mergeAlgorithm.canMerge()) {
                                break;
                            }
                            BinlogScanner mergeTo = mergeAlgorithm.mergeTo;
                            List<BinlogSender> beforeSenders = Lists.newArrayList(mergeTo.getSenders());
                            List<BinlogScanner> candidates = mergeAlgorithm.candidates;
                            int beforeSize = mergeTo.getSenders().size();
                            for (BinlogScanner scanner : candidates) {
                                mergeTo.addSenders(scanner);
                                removeScanner(scanner, false);
                            }
                            int afterSize = mergeTo.getSenders().size();
                            BINLOG_SCANNER_LOGGER.info("[mergeScanner] [type:{}] [cost = {}], [{} -> {}]. before:{}, final:{}", consumeType.name().toLowerCase(), System.currentTimeMillis() - start, beforeSize, afterSize, beforeSenders, mergeTo);
                            waitMergeScanner.remove(mergeTo);
                            waitMergeScanner.removeAll(candidates);
                        }
                    } finally {
                        waitMergeScanner.clear();
                        waitMergeScanner.notifyAll();
                    }
                }
            });
        }
    }

    private MergeAlgorithm calculate(List<BinlogScanner> scanners) {
        int maxGtidGap = DynamicConfig.getInstance().getMaxGtidGapForMergeScanner();
        int maxSenderNumPerScanner = DynamicConfig.getInstance().getMaxSenderNumPerScanner();
        return MergeAlgorithm.calculate(scanners, maxGtidGap, maxSenderNumPerScanner);
    }

    private void report() {
        if (CollectionUtils.isEmpty(getScanners())) {
            this.clearReporter();
            return;
        }
        for (Map.Entry<ConsumeType, List<BinlogScanner>> entry : scannerMap.entrySet()) {
            ConsumeType consumeType = entry.getKey();
            List<BinlogScanner> scannerList = entry.getValue();
            for (BinlogScanner scanner : scannerList) {
                Map<String, String> tags = getTags(consumeType.name().toLowerCase(), scanner.getName());
                DefaultReporterHolder.getInstance().reportReplicatorScannerSenderNum(tags, scanner.getSenders().size(), MEASUREMENT);
            }
        }
    }

    private Map<String, String> getTags(String consumeType, String scannerName) {
        tags.put("scanner", scannerName);
        tags.put("type", consumeType);
        return tags;
    }

    @Override
    public synchronized void removeScanner(BinlogScanner scanner, boolean stopSender) {
        List<BinlogScanner> scannerList = scannerMap.get(scanner.getConsumeType());
        if (scannerList == null) {
            return;
        }
        if (!scannerList.remove(scanner)) {
            return;
        }
        BINLOG_SCANNER_LOGGER.info("[removeScanner] {} removeSender: {}", scanner, stopSender);
        try {
            if (stopSender) {
                for (BinlogSender sender : scanner.getSenders()) {
                    sender.stop();
                    sender.dispose();
                }
            }
            LifecycleHelper.stopIfPossible(scanner);
            LifecycleHelper.disposeIfPossible(scanner);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            clearReporter(scanner);
            Collections.sort(scannerList);
        }
    }

    @Override
    public boolean isScannerEmpty() {
        return getScanners().isEmpty();
    }


    @Override
    public void dispose() throws Exception {
        BINLOG_SCANNER_LOGGER.info("dispose manager: {}", registryKey);
        running = false;
        for (Map.Entry<ConsumeType, List<BinlogScanner>> entry : scannerMap.entrySet()) {
            List<BinlogScanner> scanners = entry.getValue();
            for (BinlogScanner scanner : scanners) {
                LifecycleHelper.stopIfPossible(scanner);
                LifecycleHelper.disposeIfPossible(scanner);
            }
            scanners.clear();
        }
        scannerMap.clear();
        waitMergeScanner.clear();
        this.clearReporter();
        if (mergeService != null) {
            this.mergeService.shutdown();
        }
    }

    @Override
    public List<BinlogScanner> getScanners() {
        return scannerMap.values().stream().flatMap(Collection::stream).collect(Collectors.toUnmodifiableList());
    }


    private void clearReporter() {
        DefaultReporterHolder.getInstance().removeRegister(MEASUREMENT, "mha", mhaName);
    }


    private void clearReporter(BinlogScanner scanner) {
        DefaultReporterHolder.getInstance().removeRegister(MEASUREMENT, "scanner", scanner.getName());
    }
}
