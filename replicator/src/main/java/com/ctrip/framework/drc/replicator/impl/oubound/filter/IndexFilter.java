package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcIndexLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.PreviousGtidsLogEvent;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.server.common.EventReader;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogScanner;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.scanner.ScannerFilterChainContext;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * Created by jixinwang on 2023/10/11
 */
public class IndexFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private final BinlogScanner scanner;

    public IndexFilter(ScannerFilterChainContext context) {
        this.scanner = context.getScanner();
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {

        if (LogEventUtils.isIndexEvent(value.getEventType())) { //first file and skip to first previous gtid event
            if (!value.isEverSeeGtid()) {
                try {
                    trySkip(value);
                } catch (IOException e) {
                    value.setCause(e);
                }
            }
            value.setSkipEvent(true);
        }

        return doNext(value, value.isSkipEvent());
    }

    private void trySkip(OutboundLogEventContext value) throws IOException {
        FileChannel fileChannel = value.getFileChannel();
        DrcIndexLogEvent indexLogEvent = value.readIndexLogEvent();
        long currentPosition = fileChannel.position();

        List<Long> indices = indexLogEvent.getIndices();
        if (indices.size() > 1) {
            GtidSet firstGtidSet = readPreviousGtids(fileChannel, indices.get(0));
            for (int i = 1; i < indices.size(); ++i) {
                if (indices.get(i).equals(indices.get(i - 1))) {
                    restorePosition(fileChannel, indices.get(i - 1), currentPosition);
                    break;
                }
                GtidSet secondGtidSet = readPreviousGtids(fileChannel, indices.get(i));
                GtidSet stepGtidSet = secondGtidSet.subtract(firstGtidSet);
                if (stepGtidSet.isContainedWithin(scanner.getGtidSet())) {
                    logger.info("[GtidSet] update from {} to {}", firstGtidSet, secondGtidSet);
                    firstGtidSet = secondGtidSet;
                } else {  // restore to last position
                    restorePosition(fileChannel, indices.get(i - 1), currentPosition);
                    break;
                }
            }
        }
    }

    private void restorePosition(FileChannel fileChannel, long restorePosition, long currentPosition) throws IOException {
        logger.info("restorePosition is {} and currentPosition is {}", restorePosition, currentPosition);
        restorePosition = Math.max(restorePosition, currentPosition);
        fileChannel.position(restorePosition);
        logger.info("[restorePosition] set to {} finally", restorePosition);
    }

    private GtidSet readPreviousGtids(FileChannel fileChannel, long position) throws IOException {
        PreviousGtidsLogEvent previousGtidsLogEvent = new PreviousGtidsLogEvent();
        try {
            fileChannel.position(position);
            logger.info("[Update] position of fileChannel to {}", position);
            EventReader.readEvent(fileChannel, previousGtidsLogEvent);
            return previousGtidsLogEvent.getGtidSet();
        } finally {
            previousGtidsLogEvent.release();
        }
    }
}
