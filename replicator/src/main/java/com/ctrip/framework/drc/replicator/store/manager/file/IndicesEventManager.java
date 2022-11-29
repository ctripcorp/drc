package com.ctrip.framework.drc.replicator.store.manager.file;

import com.ctrip.framework.drc.core.driver.binlog.impl.DrcIndexLogEvent;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Author limingdong
 * @create 2022/11/28
 */
public class IndicesEventManager {

    private Logger logger = LoggerFactory.getLogger(IndicesEventManager.class);

    private List<Long> indices = Lists.newArrayList();

    private List<Long> notRevisedIndices = Lists.newArrayList();

    private int indicesSize = 0;  // exclude firstPreviousGtidEventPosition

    private long indexEventPosition = 0;

    private boolean everSeeDdl = false;

    private String registryKey;

    private long firstPreviousGtidEventPosition;

    private String fileName;

    public IndicesEventManager(long previousGtidPosition, String registryKey, String fileName) {
        this.firstPreviousGtidEventPosition = previousGtidPosition;
        this.registryKey = registryKey;
        this.fileName = fileName;
    }

    public DrcIndexLogEvent createIndexEvent(long startPosition) {
        indexEventPosition = startPosition;

        notRevisedIndices.add(firstPreviousGtidEventPosition);
        indices.add(firstPreviousGtidEventPosition);
        DrcIndexLogEvent indexLogEvent = createDrcIndexLogEvent();
        logger.info("[Persist] drc index log event {}:{} for {} at position {} of file {} and clear indicesSize", indices, notRevisedIndices, registryKey, indexEventPosition, fileName);

        return indexLogEvent;
    }

    public DrcIndexLogEvent updateIndexEvent(long previousGtidPosition) {
        notRevisedIndices.add(previousGtidPosition);
        if (everSeeDdl) {
            long previousPosition = previousGtidPosition;
            previousGtidPosition = indices.get(indices.size() - 1);
            logger.info("[Update] index position from {} to {} of file {}", previousPosition, previousGtidPosition, fileName);
        }
        indices.add(previousGtidPosition);
        indicesSize++;
        DrcIndexLogEvent indexLogEvent = createDrcIndexLogEvent();
        logger.info("[Persist] drc index log event {}:{} for {} at position {} of file {}", indices, notRevisedIndices, registryKey, previousGtidPosition, fileName);
        return indexLogEvent;

    }

    private DrcIndexLogEvent createDrcIndexLogEvent() {
        return new DrcIndexLogEvent(indices, notRevisedIndices, 0 , indexEventPosition);
    }

    public boolean isEverSeeDdl() {
        return everSeeDdl;
    }

    public void setEverSeeDdl(boolean everSeeDdl) {
        this.everSeeDdl = everSeeDdl;
    }

    public int getIndicesSize() {
        return indicesSize;
    }

    public long getIndexEventPosition() {
        return indexEventPosition;
    }
}
