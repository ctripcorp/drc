package com.ctrip.framework.drc.core.monitor.enums;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-07-28
 */
public enum MeasurementEnum {

    BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_MEASUREMENT("fx.drc.btdhs", true),

    META_MEASUREMENT("fx.drc.meta", true),

    DATA_CONSISTENCY_MEASUREMENT("fx.drc.data.consistency", true),

    DELAY_V2_MEASUREMENT("fx.drc.delay.v2", false),

    TRUNCATE_CONSISTENCY_MEASUREMENT("fx.drc.truncate.consistency", true);


    private String measurement;

    private boolean isBoolean;

    MeasurementEnum(String measurement, boolean isBoolean) {
        this.measurement = measurement;
        this.isBoolean = isBoolean;
    }

    public String getMeasurement() {
        return measurement;
    }

    public boolean isBoolean() {
        return isBoolean;
    }
}
