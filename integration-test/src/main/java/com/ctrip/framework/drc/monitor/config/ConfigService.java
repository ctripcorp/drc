package com.ctrip.framework.drc.monitor.config;

import com.ctrip.xpipe.config.AbstractConfigBean;
import com.google.common.collect.Lists;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.List;

/**
 * Created by jixinwang on 2020/8/17
 */
public class ConfigService extends AbstractConfigBean {

    

    private static class ConfigServiceHolder {
        private static ConfigService instance = new ConfigService();
    }

    private ConfigService() {
    }

    public static ConfigService getInstance() {
        return ConfigServiceHolder.instance;
    }

    // qconfig key
    private static final String KEY_DRC_MONITOR_MYSQL_SRC_IP = "drc.monitor.mysql.src.ip";
    private static final String KEY_DRC_MONITOR_MYSQL_SRC_PORT = "drc.monitor.mysql.src.port";
    private static final String KEY_DRC_MONITOR_MYSQL_DST_IP = "drc.monitor.mysql.dst.ip";
    private static final String KEY_DRC_MONITOR_MYSQL_DST_PORT = "drc.monitor.mysql.dst.port";
    private static final String KEY_DRC_MONITOR_MYSQL_USER = "drc.monitor.mysql.user";
    private static final String KEY_DRC_MONITOR_MYSQL_PASSWORD = "drc.monitor.mysql.password";
    private static final String KEY_AUTO_WRITE_SWITCH = "auto.write.switch";
    private static final String KEY_AUTO_BENCHMARK_SWITCH = "auto.benchmark.switch";
    private static final String KEY_AUTO_DDL_SWITCH = "auto.ddl.switch";
    private static final String KEY_AUTO_MESSENGER_SWITCH = "auto.messenger.switch";
    private static final String KEY_AUTO_DAL_SWITCH = "auto.dal.switch";
    private static final String KEY_AUTO_UNI_SWITCH = "auto.uni.switch";
    private static final String KEY_SCANNER_SWITCH = "auto.scanner.switch";

    private static final String KEY_AUTO_BI_SWITCH = "auto.bi.switch";
    private static final String KEY_AUTO_TABLE_COMPARE_SWITCH = "auto.table.compare.switch";
    private static final String KEY_AUTO_TABLE_COMPARE_EXCLUDE_MHA = "auto.table.compare.exclude.mha";
    private static final String KEY_AUTO_TABLE_COMPARE_INCLUDE_MHA = "auto.table.compare.include.mha";
    private static final String UNI_LATERAL_TRUNCATE_SWITCH = "uni.lateral.truncate.switch";
    private static final String UCS_UNIT_ROUTE = "ucs.unit.route";
    private static final String BI_LATERAL_SCHEDULE_PERIOD = "bi.lateral.schedule.period";
    private static final String KEY_DRC_MONITOR_QPS_ROUND = "drc.monitor.qps.round";
    private static final String KEY_DRC_MONITOR_QPS_SQL = "drc.monitor.qps.sql";
    private static final String KEY_DRC_MONITOR_QPS_DELETE = "drc.monitor.qps.delete";
    private static final String KEY_DRC_MONITOR_QPS_INSERT = "drc.monitor.qps.insert";
    private static final String KEY_DRC_MONITOR_GRAND_TRANSACTION_ROWS = "drc.monitor.grand.transaction.rows";
    private static final String KEY_DRC_MONITOR_GRAND_EVENT_SIZE = "drc.monitor.grand.event.size";
    private static final String KEY_DRC_ENV_TYPE = "drc.env.type";
    private static final String KEY_DRC_MONITOR_QPS_SWITCH = "drc.monitor.qps.switch";
    private static final String KEY_GRAND_TRANSACTION_SWITCH = "grand.transaction.switch";
    private static final String KEY_GRAND_EVENT_SWITCH = "grand.event.switch";
    private static final String KEY_RESULT_COMPARE_SWITCH = "result.compare.switch";
    private static final String KEY_DRC_DDL_QPS_SWITCH = "drc.ddl.qps.switch";
    private static final String KEY_DRC_DDL_QPS_ROUND = "drc.ddl.qps.round";
    private static final String KEY_GENERIC_DDL_SWITCH = "generic.ddl.switch";
    private static final String KEY_GENERIC_SINGLE_SIDE_DDL_SWITCH = "generic.single.side.ddl.switch";
    private static final String KEY_GHOST_DDL_SWITCH = "ghost.ddl.switch";
    private static final String KEY_GHOST_SINGLE_SIDE_DDL_SWITCH = "ghost.single.side.ddl.switch";
    private static final String KEY_DAL_DATA_SOURCE_SWITCH = "dal.data.source.switch";
    private static final String KEY_INTEGRATION_TEST_INSTANCE_NAME = "integration.test.instance.name";
    private static final String KEY_COMPOSITE_KEYS_DDL_SWITCH = "composite.keys.ddl.switch";
    private static final String KEY_BINLOG_MINIMAL_ROW_IMAGE_SWITCH = "binlog.minimal.row.image.switch";
    private static final String KEY_BINLOG_NOBLOB_ROW_IMAGE_SWITCH = "binlog.noblob.row.image.switch";
    private static final String KEY_BENCHMARK_TWO_SIDE_WRITE_SWITCH = "benchmark.two.side.write.switch";
    private static final String KEY_SCANNER_TEST_SHARD_NUM = "scanner.test.shard.num";


    // qconfig default value
    private static final String DEFAULT_DRC_MONITOR_MYSQL_SRC_IP = "";
    private static final String DEFAULT_DRC_MONITOR_MYSQL_SRC_PORT = "";
    private static final String DEFAULT_DRC_MONITOR_MYSQL_DST_IP = "";
    private static final String DEFAULT_DRC_MONITOR_MYSQL_DST_PORT = "";
    private static final String DEFAULT_DRC_MONITOR_MYSQL_USER = "";
    private static final String DEFAULT_DRC_MONITOR_MYSQL_PASSWORD = "";
    private static final boolean DEFAULT_AUTO_WRITE_SWITCH = false;
    private static final boolean DEFAULT_AUTO_BENCHMARK_SWITCH = false;
    private static final boolean DEFAULT_AUTO_DDL_SWITCH = false;
    private static final boolean DEFAULT_AUTO_DAL_SWITCH = false;
    private static final boolean DEFAULT_AUTO_UNI_SWITCH = false;
    private static final boolean DEFAULT_AUTO_BI_SWITCH = false;
    private static final int DEFAULT_DRC_MONITOR_QPS_ROUND = 1;
    private static final String DEFAULT_DRC_MONITOR_QPS_SQL = "";
    private static final int DEFAULT_DRC_MONITOR_QPS_DELETE = 0;
    private static final String DEFAULT_DRC_MONITOR_QPS_INSERT = "1k";
    private static final int DEFAULT_DRC_MONITOR_GRAND_TRANSACTION_ROWS = 20;
    private static final int DEFAULT_DRC_MONITOR_GRAND_EVENT_SIZE = 20;
    private static final String DEFAULT_DRC_ENV_TYPE = "fat";
    private static final boolean DEFAULT_DRC_MONITOR_QPS_SWITCH = false;
    private static final boolean DEFAULT_GRAND_TRANSACTION_SWITCH = false;
    private static final boolean DEFAULT_GRAND_EVENT_SWITCH = false;
    private static final boolean DEFAULT_RESULT_COMPARE_SWITCH = false;
    private static final boolean DEFAULT_DRC_DDL_QPS_SWITCH = false;
    private static final int DEFAULT_DRC_DDL_QPS_ROUND = 10;
    private static final boolean DEFAULT_GENERIC_DDL_SWITCH = false;
    private static final boolean DEFAULT_GENERIC_SINGLE_SIDE_DDL_SWITCH = false;
    private static final boolean DEFAULT_GHOST_DDL_SWITCH = false;
    private static final boolean DEFAULT_GHOST_SINGLE_SIDE_DDL_SWITCH = false;
    private static final boolean DEFAULT_DAL_DATA_SOURCE_SWITCH = false;
    private static final String DEFAULT_INTEGRATION_TEST_INSTANCE_NAME = "integration.test";
    private static final boolean DEFAULT_COMPOSITE_KEYS_DDL_SWITCH = false;
    private static final boolean DEFAULT_BINLOG_MINIMAL_ROW_IMAGE_SWITCH = false;
    private static final boolean DEFAULT_BINLOG_NOBLOB_ROW_IMAGE_SWITCH = false;
    private static final boolean DEFAULT_BENCHMARK_TWO_SIDE_WRITE_SWITCH =false;

    
    
    public String getDrcMonitorMysqlSrcIp() {
        return getProperty(KEY_DRC_MONITOR_MYSQL_SRC_IP, DEFAULT_DRC_MONITOR_MYSQL_SRC_IP);
    }

    public String getDrcMonitorMysqlSrcPort() {
        return getProperty(KEY_DRC_MONITOR_MYSQL_SRC_PORT, DEFAULT_DRC_MONITOR_MYSQL_SRC_PORT);
    }

    public String getDrcMonitorMysqlDstIp() {
        return getProperty(KEY_DRC_MONITOR_MYSQL_DST_IP, DEFAULT_DRC_MONITOR_MYSQL_DST_IP);
    }

    public String getDrcMonitorMysqlDstPort() {
        return getProperty(KEY_DRC_MONITOR_MYSQL_DST_PORT, DEFAULT_DRC_MONITOR_MYSQL_DST_PORT);
    }

    public String getDrcMonitorMysqlUser() {
        return getProperty(KEY_DRC_MONITOR_MYSQL_USER, DEFAULT_DRC_MONITOR_MYSQL_USER);
    }

    public String getDrcMonitorMysqlPassword() {
        return getProperty(KEY_DRC_MONITOR_MYSQL_PASSWORD, DEFAULT_DRC_MONITOR_MYSQL_PASSWORD);
    }

    public boolean getAutoWriteSwitch() {
        return getBooleanProperty(KEY_AUTO_WRITE_SWITCH, DEFAULT_AUTO_WRITE_SWITCH);
    }

    public boolean getAutoBenchmarkSwitch() {
        return getBooleanProperty(KEY_AUTO_BENCHMARK_SWITCH, DEFAULT_AUTO_BENCHMARK_SWITCH);
    }

    public boolean getAutoDdlSwitch() {
        return getBooleanProperty(KEY_AUTO_DDL_SWITCH, DEFAULT_AUTO_DDL_SWITCH);
    }

    public boolean getAutoMessengerSwitch() {
        return getBooleanProperty(KEY_AUTO_MESSENGER_SWITCH, false);
    }

    public boolean getAutoDalSwitch() {
        return getBooleanProperty(KEY_AUTO_DAL_SWITCH, DEFAULT_AUTO_DAL_SWITCH);
    }
    public boolean getAutoTableCompareSwitch() {
        return getBooleanProperty(KEY_AUTO_TABLE_COMPARE_SWITCH, false);
    }

    public int getDrcMonitorQpsRound() {
        return getIntProperty(KEY_DRC_MONITOR_QPS_ROUND, DEFAULT_DRC_MONITOR_QPS_ROUND);
    }

    public String getDrcMonitorQpsSql() {
        return getProperty(KEY_DRC_MONITOR_QPS_SQL, DEFAULT_DRC_MONITOR_QPS_SQL);
    }

    public String getDrcMonitorQpsInsert() {
        return getProperty(KEY_DRC_MONITOR_QPS_INSERT, DEFAULT_DRC_MONITOR_QPS_INSERT);
    }

    public int getDrcMonitorQpsDelete() {
        return getIntProperty(KEY_DRC_MONITOR_QPS_DELETE, DEFAULT_DRC_MONITOR_QPS_DELETE);
    }

    public String getDrcEnvType() {
        return getProperty(KEY_DRC_ENV_TYPE, DEFAULT_DRC_ENV_TYPE);
    }

    public boolean getDrcMonitorQpsSwitch() {
        return getBooleanProperty(KEY_DRC_MONITOR_QPS_SWITCH, DEFAULT_DRC_MONITOR_QPS_SWITCH);
    }

    public boolean getGrandTransactionSwitch() {
        return getBooleanProperty(KEY_GRAND_TRANSACTION_SWITCH, DEFAULT_GRAND_TRANSACTION_SWITCH);
    }

    public boolean getGrandEventSwitch() {
        return getBooleanProperty(KEY_GRAND_EVENT_SWITCH, DEFAULT_GRAND_EVENT_SWITCH);
    }

    public boolean getResultCompareSwitch() {
        return getBooleanProperty(KEY_RESULT_COMPARE_SWITCH, DEFAULT_RESULT_COMPARE_SWITCH);
    }

    public boolean getConflictBenchmarkSwitch() {
        return getBooleanProperty("conflict.benchmark.switch", false);
    }
    
    public int getConflictBenchmarkQPS() {
        return getIntProperty("conflict.benchmark.qps", 1000);
    }

    public boolean getDrcDdlQpsSwitch() {
        return getBooleanProperty(KEY_DRC_DDL_QPS_SWITCH, DEFAULT_DRC_DDL_QPS_SWITCH);
    }

    public int getDrcDdlQpsRound() {
        return getIntProperty(KEY_DRC_DDL_QPS_ROUND, DEFAULT_DRC_DDL_QPS_ROUND);
    }
    
    public boolean getGenericDdlSwitch() {
        return getBooleanProperty(KEY_GENERIC_DDL_SWITCH, DEFAULT_GENERIC_DDL_SWITCH);
    }

    public boolean getGenericSingleSideDdlSwitch() {
        return getBooleanProperty(KEY_GENERIC_SINGLE_SIDE_DDL_SWITCH, DEFAULT_GENERIC_SINGLE_SIDE_DDL_SWITCH);
    }

    public boolean getGhostDdlSwitch() {
        return getBooleanProperty(KEY_GHOST_DDL_SWITCH, DEFAULT_GHOST_DDL_SWITCH);
    }

    public boolean getGhostSingleSideDdlSwitch() {
        return getBooleanProperty(KEY_GHOST_SINGLE_SIDE_DDL_SWITCH, DEFAULT_GHOST_SINGLE_SIDE_DDL_SWITCH);
    }

    public boolean getDalDataSourceSwitch() {
        return getBooleanProperty(KEY_DAL_DATA_SOURCE_SWITCH, DEFAULT_DAL_DATA_SOURCE_SWITCH);
    }

    public String getIntegrationTestInstanceName() {
        return getProperty(KEY_INTEGRATION_TEST_INSTANCE_NAME, DEFAULT_INTEGRATION_TEST_INSTANCE_NAME);
    }

    public int getGrandTransactionRows() {
        return getIntProperty(KEY_DRC_MONITOR_GRAND_TRANSACTION_ROWS, DEFAULT_DRC_MONITOR_GRAND_TRANSACTION_ROWS);
    }

    public int getGrandEventSize() {
        return getIntProperty(KEY_DRC_MONITOR_GRAND_EVENT_SIZE, DEFAULT_DRC_MONITOR_GRAND_EVENT_SIZE);
    }

    public boolean getCompositeKeysDdlSwitch() {
        return getBooleanProperty(KEY_COMPOSITE_KEYS_DDL_SWITCH, DEFAULT_COMPOSITE_KEYS_DDL_SWITCH);
    }

    public boolean getBinlogMinimalRowImageSwitch() {
        return getBooleanProperty(KEY_BINLOG_MINIMAL_ROW_IMAGE_SWITCH, DEFAULT_BINLOG_MINIMAL_ROW_IMAGE_SWITCH);
    }

    public boolean getBinlogNoBlobRowImageSwitch() {
        return getBooleanProperty(KEY_BINLOG_NOBLOB_ROW_IMAGE_SWITCH, DEFAULT_BINLOG_NOBLOB_ROW_IMAGE_SWITCH);
    }

    public boolean getBenchmarkTwoSideWriteSwitch() {
        return getBooleanProperty(KEY_BENCHMARK_TWO_SIDE_WRITE_SWITCH, DEFAULT_BENCHMARK_TWO_SIDE_WRITE_SWITCH);
    }

    public boolean getAutoUniSwitch() {
        return getBooleanProperty(KEY_AUTO_UNI_SWITCH, DEFAULT_AUTO_UNI_SWITCH);
    }

    public boolean getAutoScannerSwitch() {
        return getBooleanProperty(KEY_SCANNER_SWITCH, DEFAULT_AUTO_UNI_SWITCH);
    }

    public boolean getAutoBiSwitch() {
        return getBooleanProperty(KEY_AUTO_BI_SWITCH, DEFAULT_AUTO_BI_SWITCH);
    }

    public boolean getUnilateralTruncateSwitch() {
        return getBooleanProperty(UNI_LATERAL_TRUNCATE_SWITCH, false);
    }

    public boolean getUcsUnitRouteSwitch() {
        return getBooleanProperty(UCS_UNIT_ROUTE, false);
    }

    public int getBiLateralSchedulePeriod() {
        return getIntProperty(BI_LATERAL_SCHEDULE_PERIOD, 3600);
    }

    public List<String> getExcludeCompareMha() {
        String property = getProperty(KEY_AUTO_TABLE_COMPARE_EXCLUDE_MHA, "");
        if (StringUtils.isEmpty(property)) {
            return Collections.emptyList();
        }
        return Lists.newArrayList(property.split(","));
    }

    public List<String> getCompareMha() {
        String property = getProperty(KEY_AUTO_TABLE_COMPARE_INCLUDE_MHA, "");
        if (StringUtils.isEmpty(property)) {
            return Collections.emptyList();
        }
        return Lists.newArrayList(property.split(","));
    }

    public int getScannerTestShardNum() {
        return getIntProperty(KEY_SCANNER_TEST_SHARD_NUM, 50);
    }

    public boolean getOldGtidSqlSwitch() {
        return getBooleanProperty("old.gtid.sql.switch", false);
    }
}
