package com.ctrip.framework.drc.core.monitor.reporter;

import com.ctrip.framework.drc.core.monitor.entity.*;
import com.ctrip.framework.drc.core.monitor.enums.AutoIncrementEnum;
import com.ctrip.framework.drc.core.monitor.enums.ConsistencyEnum;
import com.ctrip.framework.drc.core.monitor.enums.MeasurementEnum;
import com.ctrip.xpipe.api.lifecycle.Ordered;

import javax.validation.Valid;
import java.util.Map;

/**
 * @Author limingdong
 * @create 2021/10/25
 */
public interface Reporter extends Ordered {

    void reportDelay(@Valid UnidirectionalEntity unidirectionalEntity, Long delay, String measurement);
    
    void reportMessengerDelay(Map<String, String> tags,long delay,String measurement);

    void reportTraffic(@Valid TrafficEntity entity, Long bytes);

    void reportConflict(@Valid ConflictEntity conflictEntity, Long number);

    void reportTransaction(@Valid TrafficEntity entity);

    void reportTransaction(@Valid TrafficEntity entity, Long count);

    void reportAlterTable(@Valid BaseEndpointEntity entity, Long count);

    void reportResetCounter(Map<String, String> tags, Long value, String measurement);

    void reportResetCounter(Map<String, String> tags, Long value, MeasurementEnum measurementEnum);

    void resetReportCounter(Map<String, String> tags, Long value, String measurement);

    void reportGtidGapCount(@Valid GtidGapEntity gtidGapEntity, Long gap);

    void reportGtidGapRepeat(@Valid GtidGapEntity gtidGapEntity, Long repeatGapCount);

    void reportTableConsistency(@Valid ConsistencyEntity consistencyEntity, ConsistencyEnum consistencyEnum);

    void reportAutoIncrementId(BaseEntity baseEntity, AutoIncrementEnum autoIncrementEnum);

    void reportConsistency(@Valid ConsistencyEntity consistencyEntity, ConsistencyEnum consistencyEnum);

    boolean removeHistogramDelay(@Valid UnidirectionalEntity unidirectionalEntity, String measurement);

    void reportResetCounterDelay(@Valid UnidirectionalEntity unidirectionalEntity, Long delay, String measurement);

    void reportApplierTableConsistency(@Valid ConsistencyEntity consistencyEntity, ConsistencyEnum consistencyEnum);

    boolean removeRegister(Map<String, String> tags, String measurement);

    boolean removeRegister(String measurement);

    boolean removeRegister(String measurement, String key, String value);

    void reportRowsFilter(@Valid RowsFilterEntity rowsFilterEntity);

    void reportTrafficStatistic(@Valid TrafficStatisticEntity trafficStatisticEntity);

}
