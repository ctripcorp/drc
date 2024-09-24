package com.ctrip.framework.drc.replicator.impl.oubound.binlog;

import com.ctrip.framework.drc.replicator.store.manager.file.BinlogPosition;
import com.google.common.collect.Lists;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * compare file pos
 *
 * @author yongnian
 * @create 2024/9/14 16:41
 */
class MergeAlgorithmV2 {
    BinlogScanner mergeTo;
    List<BinlogScanner> candidates;

    public MergeAlgorithmV2(BinlogScanner mergeTo, List<BinlogScanner> candidates) {
        this.mergeTo = mergeTo;
        this.candidates = candidates;
    }

    public static List<MergeAlgorithmV2> calculateAll(List<BinlogScanner> scannersList, int maxPosGap, int maxSenderNumPerScanner) {
        if (scannersList == null) {
            return Collections.emptyList();
        }
        scannersList = Lists.newArrayList(scannersList);
        List<MergeAlgorithmV2> res = Lists.newArrayList();
        while (!scannersList.isEmpty()) {
            MergeAlgorithmV2 mergeAlgorithm = calculate(scannersList, maxPosGap, maxSenderNumPerScanner);
            if (!mergeAlgorithm.canMerge()) {
                break;
            }
            res.add(mergeAlgorithm);
            BinlogScanner mergeTo = mergeAlgorithm.mergeTo;
            List<BinlogScanner> candidates = mergeAlgorithm.candidates;

            scannersList.remove(mergeTo);
            scannersList.removeAll(candidates);
        }
        return res;
    }



    public static MergeAlgorithmV2 calculate(List<BinlogScanner> scannersList, int maxPosGap, int maxSenderNumPerScanner) {
        if (scannersList == null) {
            return stop();
        }
        scannersList = Lists.newArrayList(scannersList);
        while (!CollectionUtils.isEmpty(scannersList)) {
            // for each loop: find the slowest scanner, and candidates that can merge to it
            MergeAlgorithmV2 mergeAlgorithm = calculateOnce(scannersList, maxPosGap, maxSenderNumPerScanner);

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

    private static MergeAlgorithmV2 stop() {
        return new MergeAlgorithmV2(null, null);
    }

    public static MergeAlgorithmV2 calculateOnce(List<BinlogScanner> scannersList, int maxPosGap, int maxSenderNumPerScanner) {
        // 1. get slowest scanner
        Optional<BinlogScanner> slowestScannerOption = scannersList.stream().min(Comparator.comparing(BinlogScanner::getBinlogPosition));
        if (slowestScannerOption.isEmpty()) {
            return stop();
        }
        BinlogPosition slowestBinlogPos = slowestScannerOption.get().getBinlogPosition();

        List<BinlogScanner> slowScanners = scannersList.stream().sorted().filter(e -> e.getBinlogPosition().equals(slowestBinlogPos)).collect(Collectors.toList());
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
                    long gap = e.getBinlogPosition().getGap(slowestBinlogPos);
                    return gap <= maxPosGap;
                })
                .filter(e -> {
                    // check sender num
                    return e.getSenders().size() <= mergeTo.getSenders().size();
                })
                .sorted(Comparator.comparing(e -> e.getBinlogPosition().getGap(slowestBinlogPos)))
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
        return new MergeAlgorithmV2(mergeTo, candidates);
    }


    public boolean canMerge() {
        return mergeTo != null && !CollectionUtils.isEmpty(candidates);
    }
    public boolean isRelated(BinlogScanner scanner){
        return mergeTo == scanner || candidates.contains(scanner);
    }

}
