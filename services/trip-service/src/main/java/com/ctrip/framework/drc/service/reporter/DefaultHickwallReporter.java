package com.ctrip.framework.drc.service.reporter;

import com.ctrip.framework.drc.core.monitor.entity.*;
import com.ctrip.framework.drc.core.monitor.enums.AutoIncrementEnum;
import com.ctrip.framework.drc.core.monitor.enums.ConsistencyEnum;
import com.ctrip.framework.drc.core.monitor.enums.MeasurementEnum;
import com.ctrip.framework.drc.core.monitor.reporter.MetricWrapper;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.foundation.Env;
import com.ctrip.framework.foundation.Foundation;
import com.ctrip.ops.hickwall.HickwallUDPReporter;
import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.config.AbstractConfigBean;
import io.dropwizard.metrics5.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-27
 */
@Validated
public class DefaultHickwallReporter extends AbstractConfigBean implements Reporter {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected final MetricRegistry metrics = new MetricRegistry();

    private static final int REPORT_PERIOD = 30;

    private static final String HICKWALL_DOMAIN = "hickwall.domain";

    private static final String DEFAULT_HICKWALL_DOMAIN = "sink.hickwall.qa.nt.ctripcorp.com";

    private static final int HICKWALL_PORT = 8090;

    private static final Long INC_AMOUNT = 1L;

    private static final String DB_NAME = "FX";

    private static final String GTID_GAP_COUNT_MEASUREMENT = "fx.drc.gtid.gap.count";

    private static final String GTID_GAP_REPEAT_MEASUREMENT = "fx.drc.gtid.gap.repeat";

    private static final String ROWS_FILTER_TOTAL_MEASUREMENT = "fx.drc.rows.filter.total";

    private static final String ROWS_FILTER_SEND_MEASUREMENT = "fx.drc.rows.filter.send";

    private static final String TRAFFIC_STATISTIC_MEASUREMENT = "fx.drc.traffic.statistic";

    private static final String CONSISTENCY_MEASUREMENT = "fx.drc.consistency";

    private static final String TABLE_CONSISTENCY_MEASUREMENT = "fx.drc.table.consistency";

    private static final String APPLIER_TABLE_CONSISTENCY_MEASUREMENT = "fx.drc.table.consistency.applier";

    private static final String CONFLICT_MEASUREMENT = "fx.drc.conflict";

    private static final String TRAFFIC_MEASUREMENT = "fx.drc.traffic";

    private static final String TRANSACTION_MEASUREMENT = "fx.drc.transaction";

    private static final String INCREMENT_ID_MEASUREMENT = "fx.drc.increment.id";

    private static final String ALTER_TABLE_MEASUREMENT = "fx.drc.table.alter";

    private static final String APP_ID_TAG = "appid";

    private static final String APP_ID_PROPERTY = System.getProperty("app.id");

    private static final Env ENV = Foundation.server().getEnv();

    private static final Env UNKNOWN_ENV = Env.UNKNOWN;

    private Map<MetricWrapper, MetricName> metricMapper;

    private boolean isEnable = false;

    public boolean removeRegister(MetricName metricName) {
        return metrics.remove(metricName);
    }

    public boolean removeRegister(Map<String, String> tags, String measurement) {
        final MetricWrapper metricWrapper = new MetricWrapper(tags, measurement);
        MetricName metricName = metricMapper.get(metricWrapper);
        if (null == metricName) return true;
        return metrics.remove(metricName);
    }

    @Override
    public boolean removeRegister(String measurement) {
        metrics.removeMatching(
                (name, metric) -> name.getKey().equalsIgnoreCase(measurement)
        );
        return true;
    }

    @Override
    public void reportRowsFilter(RowsFilterEntity rowsFilterEntity) {
        reportResetCounter(rowsFilterEntity.getTags(), rowsFilterEntity.getTotal(), ROWS_FILTER_TOTAL_MEASUREMENT);
        reportResetCounter(rowsFilterEntity.getTags(), rowsFilterEntity.getSend(), ROWS_FILTER_SEND_MEASUREMENT);
    }

    @Override
    public void reportTrafficStatistic(TrafficStatisticEntity trafficStatisticEntity) {
        reportResetCounter(trafficStatisticEntity.getTags(), trafficStatisticEntity.getSend(), TRAFFIC_STATISTIC_MEASUREMENT);
    }

    @Override
    public int getOrder() {
        return 0;
    }

    public DefaultHickwallReporter() {
        setHickwallEnable();
        metricMapper = new ConcurrentHashMap<>();
    }

    // only for ut
    private DefaultHickwallReporter(Config config) {
        super(config);
        setHickwallEnable();
        metricMapper = new ConcurrentHashMap<>();
    }

    public MetricRegistry getMetrics() {
        return metrics;
    }

    private void setHickwallEnable() {
        try {
            if (!isEnable) {
                String hickwallDomain = getProperty(HICKWALL_DOMAIN, DEFAULT_HICKWALL_DOMAIN);
                HickwallUDPReporter.enable(
                        metrics,
                        REPORT_PERIOD,
                        TimeUnit.SECONDS,
                        hickwallDomain,
                        HICKWALL_PORT,
                        DB_NAME
                );
                isEnable = true;
            }
        } catch (Exception e) {
            logger.warn("setHickwallEnable exception: ", e);
        }
    }

    public void reportHistogram(Map<String, String> tags, Long value, String measurement) {
        MetricName metricName = getMetricName(tags, measurement);
        final Histogram histogram = metrics.histogram(metricName);
        histogram.update(value);
    }

    public void reportResetCounter(Map<String, String> tags, Long value, String measurement) {
        MetricName metricName = getMetricName(tags, measurement);
        final ResetCounter resetCounter = metrics.resetCounter(metricName);
        resetCounter.setZero(true);

        if (isBooleanMeasurement(measurement)) {
            // set the counter to 0 for every call as it is boolean
            resetCounter.getCount();
        }
        resetCounter.inc(value);
    }

    public void reportResetCounter(Map<String, String> tags, Long value, MeasurementEnum measurementEnum) {
        MetricName metricName = getMetricName(tags, measurementEnum.getMeasurement());
        final ResetCounter resetCounter = metrics.resetCounter(metricName);
        resetCounter.setZero(true);

        if (measurementEnum.isBoolean()) {
            // set the counter to 0 for every call as it is boolean
            resetCounter.getCount();
        }
        resetCounter.inc(value);
    }

    private boolean isBooleanMeasurement(String measurement) {
        return measurement.equalsIgnoreCase(CONSISTENCY_MEASUREMENT) || measurement.equalsIgnoreCase(TABLE_CONSISTENCY_MEASUREMENT) || measurement.equalsIgnoreCase(APPLIER_TABLE_CONSISTENCY_MEASUREMENT) || measurement.equalsIgnoreCase(GTID_GAP_REPEAT_MEASUREMENT) || measurement.equalsIgnoreCase(INCREMENT_ID_MEASUREMENT) || measurement.equalsIgnoreCase(GTID_GAP_COUNT_MEASUREMENT);
    }

    public void reportCounter(Map<String, String> tags, Long value, String measurement) {
        MetricName metricName = getMetricName(tags, measurement);
        final Counter counter = metrics.counter(metricName);
        counter.inc(value);
    }

    // counter and clear every call
    public void resetReportCounter(Map<String, String> tags, Long value, String measurement) {
        MetricName metricName = getMetricName(tags, measurement);
        final Counter counter = metrics.counter(metricName);
        long count = counter.getCount();
        counter.dec(count);
        counter.inc(value);
    }

    public void reportDelay(@Valid UnidirectionalEntity unidirectionalEntity, Long delay, String measurement) {
        reportHistogram(unidirectionalEntity.getTags(), delay, measurement);
    }

    @Override
    public void reportMessengerDelay(Map<String, String> tags, long delay, String measurement) {
        reportHistogram(tags,delay,measurement);
    }

    public boolean removeHistogramDelay(@Valid UnidirectionalEntity unidirectionalEntity, String measurement) {
        MetricName metricName = getMetricName(unidirectionalEntity.getTags(), measurement);
        return metrics.remove(metricName);
    }

    public void reportResetCounterDelay(@Valid UnidirectionalEntity unidirectionalEntity, Long delay, String measurement) {
        final Map<String, String> tags = unidirectionalEntity.getTags();
        String delaySumMeasurement = measurement + ".value";
        String delayCountMeasurement = measurement + ".count";
        reportResetCounter(tags, delay, delaySumMeasurement);
        reportResetCounter(tags, INC_AMOUNT, delayCountMeasurement);
    }

    private void reportGtid(GtidGapEntity gtidGapEntity, Long value, String measurement) {
        resetReportCounter(gtidGapEntity.getTags(),value,measurement);
    }

    public void reportGtidGapCount(@Valid GtidGapEntity gtidGapEntity, Long gap) {
        reportGtid(gtidGapEntity, gap, GTID_GAP_COUNT_MEASUREMENT);
    }

    public void reportGtidGapRepeat(@Valid GtidGapEntity gtidGapEntity, Long repeatGapCount) {
        reportGtid(gtidGapEntity, repeatGapCount, GTID_GAP_REPEAT_MEASUREMENT);
    }

    public void reportConsistencyBasic(@Valid ConsistencyEntity consistencyEntity, ConsistencyEnum consistencyEnum, String measurement) {
        reportResetCounter(consistencyEntity.getTags(), consistencyEnum.getCode(), measurement);
    }

    /**
     * report data consistency
     */
    public void reportConsistency(@Valid ConsistencyEntity consistencyEntity, ConsistencyEnum consistencyEnum) {
        reportResetCounter(consistencyEntity.getTags(), consistencyEnum.getCode(), MeasurementEnum.DATA_CONSISTENCY_MEASUREMENT);
    }

    /**
     * report the columns consistency between tables in two dc
     */
    public void reportTableConsistency(@Valid ConsistencyEntity consistencyEntity, ConsistencyEnum consistencyEnum) {
        resetReportCounter(consistencyEntity.getTags(),consistencyEnum.getCode(),TABLE_CONSISTENCY_MEASUREMENT);
    }

    /**
     * report the columns consistency between applier and table in dc
     */
    public void reportApplierTableConsistency(@Valid ConsistencyEntity consistencyEntity, ConsistencyEnum consistencyEnum) {
        reportConsistencyBasic(consistencyEntity, consistencyEnum, APPLIER_TABLE_CONSISTENCY_MEASUREMENT);
    }

    public void reportConflict(@Valid ConflictEntity conflictEntity, Long number) {
        reportResetCounter(conflictEntity.getTags(), number, CONFLICT_MEASUREMENT);
    }

    public void reportTraffic(@Valid TrafficEntity entity, Long bytes) {
        reportResetCounter(entity.getTags(), bytes, TRAFFIC_MEASUREMENT);
    }

    public void reportTransaction(@Valid TrafficEntity entity) {
        this.reportTransaction(entity, INC_AMOUNT);
    }

    public void reportTransaction(@Valid TrafficEntity entity, Long count) {
        reportResetCounter(entity.getTags(), count, TRANSACTION_MEASUREMENT);
    }

    /**
     * report if the setting for auto increment id is correct
     */
    public void reportAutoIncrementId(BaseEntity baseEntity, AutoIncrementEnum autoIncrementEnum) {
        reportResetCounter(baseEntity.getTags(), autoIncrementEnum.getCode(), INCREMENT_ID_MEASUREMENT);
    }

    public void reportAlterTable(@Valid BaseEndpointEntity entity, Long count) {
        reportResetCounter(entity.getTags(), count, ALTER_TABLE_MEASUREMENT);
    }

    private MetricName getMetricName(Map<String, String> tags, String measurement) {
        final MetricWrapper metricWrapper = new MetricWrapper(tags, measurement);
        MetricName metricName;
        if (null == (metricName = metricMapper.get(metricWrapper))) {
            metricName = MetricName.build(measurement).tagged(tags);
            if (UNKNOWN_ENV == ENV) {
                metricName = metricName.tagged(APP_ID_TAG, APP_ID_PROPERTY);
            }
            metricMapper.put(metricWrapper, metricName);
        }
        return metricName;
    }

}
