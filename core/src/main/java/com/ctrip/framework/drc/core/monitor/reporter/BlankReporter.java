package com.ctrip.framework.drc.core.monitor.reporter;

import com.ctrip.framework.drc.core.monitor.entity.*;
import com.ctrip.framework.drc.core.monitor.enums.AutoIncrementEnum;
import com.ctrip.framework.drc.core.monitor.enums.ConsistencyEnum;
import com.ctrip.framework.drc.core.monitor.enums.MeasurementEnum;


import java.util.Map;

/**
 * @Author limingdong
 * @create 2021/12/1
 */
public class BlankReporter implements Reporter {

    @Override
    public void reportDelay(UnidirectionalEntity unidirectionalEntity, Long delay, String measurement) {

    }

    @Override
    public void reportTraffic(TrafficEntity entity, Long bytes) {

    }

    @Override
    public void reportConflict(ConflictEntity conflictEntity, Long number) {

    }

    @Override
    public void reportTransaction(TrafficEntity entity) {

    }

    @Override
    public void reportTransaction(TrafficEntity entity, Long count) {

    }

    @Override
    public void reportAlterTable(BaseEndpointEntity entity, Long count) {

    }

    @Override
    public void reportResetCounter(Map<String, String> tags, Long value, String measurement) {

    }

    @Override
    public void reportResetCounter(Map<String, String> tags, Long value, MeasurementEnum measurementEnum) {
        
    }

    @Override
    public void resetReportCounter(Map<String, String> tags, Long value, String measurement) {

    }

    @Override
    public void reportGtidGapCount(GtidGapEntity gtidGapEntity, Long gap) {

    }

    @Override
    public void reportGtidGapRepeat(GtidGapEntity gtidGapEntity, Long repeatGapCount) {

    }

    @Override
    public void reportTableConsistency(ConsistencyEntity consistencyEntity, ConsistencyEnum consistencyEnum) {

    }

    @Override
    public void reportAutoIncrementId(BaseEntity baseEntity, AutoIncrementEnum autoIncrementEnum) {

    }

    @Override
    public void reportConsistency(ConsistencyEntity consistencyEntity, ConsistencyEnum consistencyEnum) {

    }

    @Override
    public boolean removeHistogramDelay(UnidirectionalEntity unidirectionalEntity, String measurement) {
        return false;
    }

    @Override
    public void reportResetCounterDelay(UnidirectionalEntity unidirectionalEntity, Long delay, String measurement) {

    }

    @Override
    public void reportApplierTableConsistency(ConsistencyEntity consistencyEntity, ConsistencyEnum consistencyEnum) {

    }

    @Override
    public boolean removeRegister(Map<String, String> tags, String measurement) {
        return false;
    }

    @Override
    public int getOrder() {
        return 1;
    }
}
