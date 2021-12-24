package com.ctrip.framework.drc.service.reporter;

import com.ctrip.framework.drc.core.monitor.entity.*;
import com.ctrip.framework.drc.core.monitor.enums.*;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.foundation.Env;
import com.ctrip.framework.foundation.Foundation;
import io.dropwizard.metrics5.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;

import static com.ctrip.framework.drc.core.monitor.enums.MeasurementEnum.BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_MEASUREMENT;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-01-08
 */
public class DefaultHickwallReporterTest {

    private static final int REPORT_PERIOD = 10;

    private static final String DELAY_MEASUREMENT = "fx.drc.delay";

    private static final String DELAY_VALUE_MEASUREMENT = "fx.drc.delay.value";

    private static final String DELAY_COUNT_MEASUREMENT = "fx.drc.delay.count";

    private static final String GTID_GAP_COUNT_MEASUREMENT = "fx.drc.gtid.gap.count";

    private static final String GTID_GAP_REPEAT_MEASUREMENT = "fx.drc.gtid.gap.repeat";

    private static final String TABLE_CONSISTENCY_MEASUREMENT = "fx.drc.table.consistency";

    private static final String APPLIER_TABLE_CONSISTENCY_MEASUREMENT = "fx.drc.table.consistency.applier";

    private static final String CONFLICT_MEASUREMENT = "fx.drc.conflict";

    private static final String TRAFFIC_MEASUREMENT = "fx.drc.traffic";

    private static final String TRANSACTION_MEASUREMENT = "fx.drc.transaction";

    private static final String INCREMENT_ID_MEASUREMENT = "fx.drc.increment.id";

    private static final String ALTER_TABLE_MEASUREMENT = "fx.drc.table.alter";

    private static final String UUID_ERROR_NUM_MEASUREMENT = "fx.drc.uuid.errorNums";

    private static final String APP_ID_TAG = "appid";

    private static final String APP_ID_PROPERTY = "app.id";

    private static final Long CLUSTER_APP_ID = 12345678910L;

    private static final String BU = "test_bu";

    private static final String LOCAL_DC_NAME = "test_dc";

    private static final String DEST_DC_NAME = "test_dest_dc";

    private static final String CLUSTER_NAME = "test_cluster";

    private static final String IP = "127.0.0.1";

    private static final int PORT = 1000;

    private static final String APPLIER_IP = "127.0.0.1";

    private static final int APPLIER_PORT = 1111;

    private static final String MYSQL_IP = "127.0.0.1";

    private static final int MYSQL_PORT = 3333;

    private static final String DEST_MYSQL_IP = "127.0.0.1";

    private static final int DEST_MYSQL_PORT = 4444;

    private static final String UUID = "1234-1234567";

    private DefaultHickwallReporter reporter = (DefaultHickwallReporter) DefaultReporterHolder.getInstance();

    private MetricRegistry metrics;

    private Random random;

    private BaseEntity baseEntity;

    private UnidirectionalEntity unidirectionalEntity;

    private GtidGapEntity gtidGapEntity;

    private ConsistencyEntity consistencyEntity;

    private ConflictEntity conflictEntity;

    private TrafficEntity trafficEntity;

    private BaseEndpointEntity baseEndpointEntity;

    @Before
    public void setUp() {

        /**
         * for test only, no need to call getMetrics() in dev
         */
        metrics = reporter.getMetrics();
        random = new Random();

        baseEntity = new BaseEntity(CLUSTER_APP_ID, BU, LOCAL_DC_NAME, CLUSTER_NAME, null, null);

        unidirectionalEntity = new UnidirectionalEntity.Builder()
                .clusterAppId(CLUSTER_APP_ID)
                .buName(BU)
                .srcDcName(LOCAL_DC_NAME)
                .destDcName(DEST_DC_NAME)
                .clusterName(CLUSTER_NAME)
                .build();

        /**
         * for reportGtidCount and reportGtidRepeat
         */
        gtidGapEntity = new GtidGapEntity.Builder()
                .clusterAppId(CLUSTER_APP_ID)
                .buName(BU)
                .dcName(LOCAL_DC_NAME)
                .clusterName(CLUSTER_NAME)
                .mysqlIp(MYSQL_IP)
                .mysqlPort(MYSQL_PORT)
                .uuid(UUID)
                .build();

        consistencyEntity = new ConsistencyEntity.Builder()
                .clusterAppId(CLUSTER_APP_ID)
                .buName(BU)
                .srcDcName(LOCAL_DC_NAME)
                .destDcName(DEST_DC_NAME)
                .clusterName(CLUSTER_NAME)
                .srcMysqlIp(MYSQL_IP)
                .srcMysqlPort(MYSQL_PORT)
                .destMysqlIp(DEST_MYSQL_IP)
                .destMysqlPort(DEST_MYSQL_PORT)
                .build();

        /**
         * for reportConflict
         */
        conflictEntity = new ConflictEntity.Builder()
                .clusterAppId(CLUSTER_APP_ID)
                .buName(BU)
                .dcName(LOCAL_DC_NAME)
                .clusterName(CLUSTER_NAME)
                .applierIp(APPLIER_IP)
                .applierPort(APPLIER_PORT)
                .mySqlIp(MYSQL_IP)
                .mysqlPort(MYSQL_PORT)
                .build();

        /**
         * reportTraffic
         */
        trafficEntity = new TrafficEntity.Builder()
                .clusterAppId(CLUSTER_APP_ID)
                .buName(BU)
                .dcName(LOCAL_DC_NAME)
                .clusterName(CLUSTER_NAME)
                .ip(IP)
                .port(PORT)
                .module(ModuleEnum.REPLICATOR.getDescription())
                .direction(DirectionEnum.IN.getDescription())
                .build();

        /**
         * reportTransaction
         */
        baseEndpointEntity = new BaseEndpointEntity.Builder()
                .clusterAppId(CLUSTER_APP_ID)
                .buName(BU)
                .dcName(LOCAL_DC_NAME)
                .clusterName(CLUSTER_NAME)
                .ip(IP)
                .port(PORT)
                .build();
    }

    @Test
    public void testReportResetCounter() {
        Map<String, String> tags = new HashMap<>() {{
            put("hello", "world");
            put("foo", "bar");
        }};

        Long expected = 0L;
        final long start = System.currentTimeMillis();
        for (int i = 0; i < 100; ++i) {
            final long num = random.nextInt(1000000);
            expected += num;
            reporter.reportResetCounter(tags, num, BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_MEASUREMENT.getMeasurement());
        }
        final long end = System.currentTimeMillis();

        final ResetCounter resetCounter = getResetCounter(tags, BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_MEASUREMENT.getMeasurement());

        final Long actual = resetCounter.getCount();
        Assert.assertEquals(expected.equals(actual), (end - start) < REPORT_PERIOD * 1000);
    }

    @Test
    public void testReportHistogramDelay() {
        Long expectedSum = 0L;
        for (int i = 0; i < 100; ++i) {
            final long num = random.nextLong();
            expectedSum += num;
            reporter.reportDelay(unidirectionalEntity, num, DELAY_MEASUREMENT);
        }

        final Histogram histogram = getHistogram(unidirectionalEntity.getTags(), DELAY_MEASUREMENT);

        final Long actualSum = histogram.getSum();
        final long actualCount = histogram.getCount();
        Assert.assertEquals(expectedSum, actualSum);
        Assert.assertEquals(100, actualCount);
    }

    @Test
    public void testRemoveHistogramDelay() {
        reporter.reportDelay(unidirectionalEntity, 1L, DELAY_MEASUREMENT);
        Assert.assertTrue(reporter.removeHistogramDelay(unidirectionalEntity, DELAY_MEASUREMENT));
    }

    @Test
    public void testReportResetCounterDelay() {
        Long expectedSum = 0L;
        Long expectedCount = 0L;
        final long start = System.currentTimeMillis();
        for (int i = 0; i < 100; ++i) {
            final long num = random.nextLong();
            expectedSum += num;
            expectedCount++;
            reporter.reportResetCounterDelay(unidirectionalEntity, num, DELAY_MEASUREMENT);
        }
        final long end = System.currentTimeMillis();

        final Map<String, String> tags = unidirectionalEntity.getTags();
        final ResetCounter resetCounter = getResetCounter(tags, DELAY_VALUE_MEASUREMENT);
        final ResetCounter resetCounter1 = getResetCounter(tags, DELAY_COUNT_MEASUREMENT);

        final long actualSum = resetCounter.getCount();
        final long actualCount = resetCounter1.getCount();
        Assert.assertEquals(expectedSum.equals(actualSum), (end - start) < REPORT_PERIOD * 1000);
        Assert.assertEquals(expectedCount.equals(actualCount), (end - start) < REPORT_PERIOD * 1000);
    }

    @Test
    public void testReportGtidGapCount() {
        Long expected = 0L;
        final long start = System.currentTimeMillis();
        for (int i = 0; i < 100; ++i) {
            final long num = random.nextLong();
            expected = num;
            reporter.reportGtidGapCount(gtidGapEntity, num);
        }
        final long end = System.currentTimeMillis();

        final ResetCounter resetCounter = getResetCounter(gtidGapEntity.getTags(), GTID_GAP_COUNT_MEASUREMENT);

        final Long actual = resetCounter.getCount();
        Assert.assertEquals(expected.equals(actual), (end - start) < REPORT_PERIOD * 1000);
    }

    @Test
    public void testReportGtidGapRepeat() {
        Long expected = 0L;
        final long start = System.currentTimeMillis();
        for (int i = 0; i < 100; ++i) {
            final long num = random.nextLong();
            expected = num;
            reporter.reportGtidGapRepeat(gtidGapEntity, num);
        }
        final long end = System.currentTimeMillis();

        final ResetCounter resetCounter = getResetCounter(gtidGapEntity.getTags(), GTID_GAP_REPEAT_MEASUREMENT);

        final Long actual = resetCounter.getCount();
        Assert.assertEquals(expected.equals(actual), (end - start) < REPORT_PERIOD * 1000);
    }

    @Test
    public void testReportConsistency() {
        Long expected = 1L;
        final long start = System.currentTimeMillis();
        for (int i = 0; i < 100; ++i) {
            reporter.reportConsistency(consistencyEntity, ConsistencyEnum.NON_CONSISTENT);
        }
        final long end = System.currentTimeMillis();

        final ResetCounter resetCounter = getResetCounter(consistencyEntity.getTags(), MeasurementEnum.DATA_CONSISTENCY_MEASUREMENT.getMeasurement());

        final Long actual = resetCounter.getCount();
        Assert.assertEquals(expected.equals(actual), (end - start) < REPORT_PERIOD * 1000);
    }

    @Test
    public void testReportTableConsistency() {
        Long expected = 1L;
        final long start = System.currentTimeMillis();
        for (int i = 0; i < 100; ++i) {
            reporter.reportTableConsistency(consistencyEntity, ConsistencyEnum.NON_CONSISTENT);
        }
        final long end = System.currentTimeMillis();

        final ResetCounter resetCounter = getResetCounter(consistencyEntity.getTags(), TABLE_CONSISTENCY_MEASUREMENT);

        final Long actual = resetCounter.getCount();
        Assert.assertEquals(expected.equals(actual), (end - start) < REPORT_PERIOD * 1000);
    }

    @Test
    public void testReportApplierTableConsistency() {
        Long expected = 1L;
        final long start = System.currentTimeMillis();
        for (int i = 0; i < 100; ++i) {
            reporter.reportApplierTableConsistency(consistencyEntity, ConsistencyEnum.NON_CONSISTENT);
        }
        final long end = System.currentTimeMillis();

        final ResetCounter resetCounter = getResetCounter(consistencyEntity.getTags(), APPLIER_TABLE_CONSISTENCY_MEASUREMENT);

        final Long actual = resetCounter.getCount();
        Assert.assertEquals(expected.equals(actual), (end - start) < REPORT_PERIOD * 1000);
    }

    @Test
    public void testReportConflict() {
        Long expected = 0L;
        final long start = System.currentTimeMillis();
        for (int i = 0; i < 100; ++i) {
            final long num = random.nextLong();
            expected += num;
            reporter.reportConflict(conflictEntity, num);
        }
        final long end = System.currentTimeMillis();

        final ResetCounter resetCounter = getResetCounter(conflictEntity.getTags(), CONFLICT_MEASUREMENT);

        final Long actual = resetCounter.getCount();
        Assert.assertEquals(expected.equals(actual), (end - start) < REPORT_PERIOD * 1000);
    }

    @Test
    public void testReportTraffic() {
        Long expected = 0L;
        final long start = System.currentTimeMillis();
        for (int i = 0; i < 100; ++i) {
            final long num = random.nextLong();
            expected += num;
            reporter.reportTraffic(trafficEntity, num);
        }
        final long end = System.currentTimeMillis();

        final ResetCounter resetCounter = getResetCounter(trafficEntity.getTags(), TRAFFIC_MEASUREMENT);

        final Long actual = resetCounter.getCount();
        Assert.assertEquals(expected.equals(actual), (end - start) < REPORT_PERIOD * 1000);
    }

    @Test
    public void testReportTransaction() {
        Long expected = 0L;
        final long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; ++i) {
            expected++;
            reporter.reportTransaction(trafficEntity);
        }
        final long end = System.currentTimeMillis();

        final ResetCounter resetCounter = getResetCounter(trafficEntity.getTags(), TRANSACTION_MEASUREMENT);

        final Long actual = resetCounter.getCount();
        Assert.assertEquals(expected.equals(actual), (end - start) < REPORT_PERIOD * 1000);
    }

    @Test
    public void testReportAutoIncrementId() {
        Long expected = 1L;
        final long start = System.currentTimeMillis();
        for (int i = 0; i < 100; ++i) {
            reporter.reportAutoIncrementId(baseEntity, AutoIncrementEnum.INCORRECT);
        }
        final long end = System.currentTimeMillis();

        final ResetCounter resetCounter = getResetCounter(baseEntity.getTags(), INCREMENT_ID_MEASUREMENT);

        final Long actual = resetCounter.getCount();
        Assert.assertEquals(expected.equals(actual), (end - start) < REPORT_PERIOD * 1000);
    }

    @Test
    public void testReportAlterTable() throws InterruptedException {
        Long expected = 3L;
        reporter.reportAlterTable(baseEndpointEntity, 1L);
        reporter.reportAlterTable(baseEndpointEntity, 2L);

        final ResetCounter resetCounter = getResetCounter(baseEndpointEntity.getTags(), ALTER_TABLE_MEASUREMENT);
        final Long actual = resetCounter.getCount();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testResetReportCounter() {
        Long expected = 1L;
        reporter.resetReportCounter(baseEndpointEntity.getTags(), 2L, UUID_ERROR_NUM_MEASUREMENT);
        reporter.resetReportCounter(baseEndpointEntity.getTags(), 1L, UUID_ERROR_NUM_MEASUREMENT);
        final Counter counter = getCounter(baseEndpointEntity.getTags(), UUID_ERROR_NUM_MEASUREMENT);
        final Long actual = counter.getCount();
        Assert.assertEquals(expected, actual);

    }

    private ResetCounter getResetCounter(Map<String, String> tags, String measurement) {
        MetricName metricName = MetricName.build(measurement).tagged(tags);
        if (Env.UNKNOWN == Foundation.server().getEnv()) {
            metricName = metricName.tagged(APP_ID_TAG, System.getProperty(APP_ID_PROPERTY));
        }

        final SortedMap<MetricName, ResetCounter> resetCounters = metrics.getResetCounters();
        return resetCounters.get(metricName);
    }

    private Histogram getHistogram(Map<String, String> tags, String measurement) {
        MetricName metricName = MetricName.build(measurement).tagged(tags);
        if (Env.UNKNOWN == Foundation.server().getEnv()) {
            metricName = metricName.tagged(APP_ID_TAG, System.getProperty(APP_ID_PROPERTY));
        }

        final SortedMap<MetricName, Histogram> histograms = metrics.getHistograms();
        return histograms.get(metricName);
    }

    private Counter getCounter(Map<String, String> tags, String measurement) {
        MetricName metricName = MetricName.build(measurement).tagged(tags);
        if (Env.UNKNOWN == Foundation.server().getEnv()) {
            metricName = metricName.tagged(APP_ID_TAG, System.getProperty(APP_ID_PROPERTY));
        }

        final SortedMap<MetricName, Counter> counters = metrics.getCounters();
        return counters.get(metricName);
    }
}
