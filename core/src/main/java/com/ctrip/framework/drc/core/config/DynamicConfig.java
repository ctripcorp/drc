package com.ctrip.framework.drc.core.config;

import com.ctrip.xpipe.config.AbstractConfigBean;
import org.apache.commons.lang3.StringUtils;

import static com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager.MAX_ACTIVE;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.SOCKET_TIMEOUT;

/**
 * Created by jixinwang on 2022/7/11
 */
public class DynamicConfig extends AbstractConfigBean {

    private static final String CONCURRENCY = "scheme.clone.task.concurrency.%s";
    private static final String SNAPSHOT_CONCURRENCY = "scheme.snapshot.task.concurrency.%s";
    private static final String SCANNER_SENDER_NUM_MAX = "binlog.scanner.sender.max";
    private static final String SCANNER_SENDER_NUM_MAX_KEY = SCANNER_SENDER_NUM_MAX+".%s";
    private static final String SCANNER_NUM_MAX = "binlog.scanner.max";
    private static final String SCANNER_MERGE_GTID_GAP_MAX = "binlog.scanner.merge.gtid.gap.max";
    private static final String SCANNER_MERGE_PERIOD_MILLI = "binlog.scanner.merge.period";
    private static final String SCANNER_SPLIT_EVENT_THRESHOLD = "binlog.scanner.split.event.threshold";
    private static final String CONSOLE_LOG_DELAY_THRESHOLD = "console.log.delay.threshold";

    private static final String DATASOURCE_SOCKET_TIMEOUT = "datasource.socket.timeout";

    private static final String TABLE_PARTITION_SWITCH = "table.partition.switch";

    private static final String INDEPENDENT_EMBEDDED_MYSQL_SWITCH = "independent.embedded.mysql.switch";

    private static final String INDEPENDENT_EMBEDDED_MYSQL_SWITCH_KEY = INDEPENDENT_EMBEDDED_MYSQL_SWITCH + ".%s";
    private static final String DELAY_EVENT_OLD_SWITCH = "delay.event.old.switch";
    private static final String DRC_DB_DELAY_MEASUREMENT = "drc.db.delay.measurement";

    private static final String PURGED_GTID_SET_CHECK_SWITCH = "purged.gtid.set.check.switch";

    private static final String RECEIVE_CHECK_SWITCH = "receive.check.switch";
    private static final String SCHEMA_MANAGER_CACHE_DISABLE_SWITCH = "schema.manager.snapshot.cache.disable.switch";
    private static final String REPLICATOR_SKIP_UNSUPPORTED_SCHEMA = "replicator.skip.unsupported.schema";
    private static final String REPLICATOR_SCALE_OUT_KEY = "replicator.scale.out.%s";


    private static final String TRAFFIC_COUNT_CHANGE = "traffic.count.change";

    private static final String CM_NOTIFY_THREAD = "cm.notify.thread";
    private static final String CM_NOTIFY_HTTPS_SWITCH = "cm.notify.https.switch";
    private static final String CM_NOTIFY_ASYNC_SWITCH = "cm.notify.async.switch";
    

    private DynamicConfig() {}

    public int getMergeCheckPeriodMilli() {
        return getIntProperty(SCANNER_MERGE_PERIOD_MILLI, 1000);
    }

    public int getScannerSplitThreshold() {
        return getIntProperty(SCANNER_SPLIT_EVENT_THRESHOLD, 8000);
    }

    public long getLogDelayDetailThresholdMillis() {
        return getIntProperty(CONSOLE_LOG_DELAY_THRESHOLD, 500);
    }

    public boolean getCMNotifyAsyncSwitch() {
        return getBooleanProperty(CM_NOTIFY_ASYNC_SWITCH, false);
    }

    private static class ConfigHolder {
        public static final DynamicConfig INSTANCE = new DynamicConfig();
    }

    public static DynamicConfig getInstance() {
        return ConfigHolder.INSTANCE;
    }

    public int getConcurrency(String key) {
        return getIntProperty(String.format(CONCURRENCY, key), MAX_ACTIVE);
    }

    public long getBinlogScaleOutNum(String registryKey, long defaultValue) {
        Long longProperty = getLongProperty(String.format(REPLICATOR_SCALE_OUT_KEY, registryKey), defaultValue);
        // must be positive
        return Math.max(1L, longProperty);
    }


    public int getMaxSenderNumPerScanner(String key) {
        int defaultNum = getIntProperty(SCANNER_SENDER_NUM_MAX, 20);
        return getIntProperty(String.format(SCANNER_SENDER_NUM_MAX_KEY, key), defaultNum);
    }

    public int getMaxScannerNumPerMha() {
        return getIntProperty(SCANNER_NUM_MAX, 100);
    }

    public int getMaxGtidGapForMergeScanner() {
        return getIntProperty(SCANNER_MERGE_GTID_GAP_MAX, 10000);
    }

    public int getSnapshotTaskConcurrency() {
        return getIntProperty(SNAPSHOT_CONCURRENCY, 5);
    }
    public int getDatasourceSocketTimeout() {
        return getIntProperty(DATASOURCE_SOCKET_TIMEOUT, SOCKET_TIMEOUT);
    }

    public boolean getTablePartitionSwitch() {
        return getBooleanProperty(TABLE_PARTITION_SWITCH, true);
    }

    public boolean getIndependentEmbeddedMySQLSwitch(String key) {
        String value = getProperty(String.format(INDEPENDENT_EMBEDDED_MYSQL_SWITCH_KEY, key));
        if (StringUtils.isBlank(value)) {
            return getBooleanProperty(INDEPENDENT_EMBEDDED_MYSQL_SWITCH, false);
        }
        return Boolean.parseBoolean(value);
    }

    public boolean getSkipUnsupportedTableSwitch() {
        return getBooleanProperty(REPLICATOR_SKIP_UNSUPPORTED_SCHEMA, false);
    }


    public boolean getPurgedGtidSetCheckSwitch() {
        String value = getProperty(PURGED_GTID_SET_CHECK_SWITCH);
        if (StringUtils.isBlank(value)) {
            return getBooleanProperty(PURGED_GTID_SET_CHECK_SWITCH, false);
        }
        return Boolean.parseBoolean(value);
    }

    public boolean getReceiveCheckSwitch() {
        return getBooleanProperty(RECEIVE_CHECK_SWITCH, false);
    }

    public boolean getDisableSnapshotCacheSwitch() {
        return getBooleanProperty(SCHEMA_MANAGER_CACHE_DISABLE_SWITCH, true);
    }

    public boolean getOldDelayEventProcessSwitch() {
        return getBooleanProperty(DELAY_EVENT_OLD_SWITCH, true);
    }

    public String getDrcDbDelayMeasurement() {
        return getProperty(DRC_DB_DELAY_MEASUREMENT, "fx.drc.delay");
    }

    public boolean getTrafficCountChangeSwitch() {
        return getBooleanProperty(TRAFFIC_COUNT_CHANGE, false);
    }

    public int getCmNotifyThread() {
        return getIntProperty(CM_NOTIFY_THREAD, 50);
    }

    public boolean getCMNotifyHttpsSwitch() {
        return getBooleanProperty(CM_NOTIFY_HTTPS_SWITCH, false);
    }
    
}
