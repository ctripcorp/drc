package com.ctrip.framework.drc.core.server.config;

import com.ctrip.framework.drc.core.server.utils.IpUtils;
import com.ctrip.xpipe.utils.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mingdongli
 * 2019/10/11 上午11:02.
 */
public class SystemConfig {

    public static final String LOCAL_SERVER_ADDRESS = IpUtils.getFistNonLocalIpv4ServerAddress();

    public static final String OPT_PATH = "/opt";

    public static final String DATA_PATH = "/data/drc";

    public static final String REPLICATOR_PATH = DATA_PATH + "/replicator/";

    public static final String KEY_REPLICATOR_PATH ="drc.replicator.data.log";

    public static final String APPLIER_PATH = OPT_PATH + DATA_PATH + "/applier/";

    public static final String VALIDATION_PATH = OPT_PATH + DATA_PATH + "/validation/";

    public static final String LOCAL_CONFIG_PATH = DATA_PATH + "/config/";

    public static final String REPLICATOR_WHITE_LIST = "drc.replicator.uuid.whitelist";

    public static final String REPLICATOR_LOCAL_SCHEMA_MANAGER = "drc.replicator.local.schemamanager";

    public static final String REPLICATOR_FILE_LIMIT = "drc.replicator.file.limit";

    public static final String REPLICATOR_FILE_FIRST = "drc.replicator.binlog.firstfile";

    public static final String JDBC_URL_PARAM = "?allowMultiQueries=true&useSSL=false&useUnicode=true&characterEncoding=UTF-8";

    public static final String DRC_PROJECT = "DRC";

    public static final String REPLICATOR_INIT_GTIDSET = "drc.replicator.init.gtidset";

    public static final String REPLICATOR_BINLOG_PURGE_SCALE_OUT = "drc.replicator.binlog.scaleout";

    public static final String PREVIOUS_GTID_INTERVAL = "drc.replicator.previousgtid.interval";

    public static final String JDBC_PREFIX = "jdbc:mysql://%s:";

    public static final int CONNECTION_TIMEOUT = 2000;

    public static final int SOCKET_TIMEOUT = 10000;

    public static int TRANSACTION_BUFFER_SIZE = 1024 * 8;

    public static final String JDBC_URL_FORMAT = JDBC_PREFIX + "%d" + SystemConfig.JDBC_URL_PARAM;

    public static final int MASTER_HEARTBEAT_PERIOD_SECONDS = 10;

    public static final int CONNECTION_IDLE_TIMEOUT_SECOND = MASTER_HEARTBEAT_PERIOD_SECONDS * 3;

    public static final long SLOW_COMMIT_THRESHOLD = 500L;

    public static final String DOT = ".";

    public static final String COMMA = ",";

    public static final String SEMICOLON = ";";

    public static final String DRC_MONITOR_SCHEMA_NAME = "drcmonitordb";

    public static final String DRC_DELAY_MONITOR_TABLE_NAME = "delaymonitor";

    public static final String DRC_DELAY_MONITOR_TABLE_PREFIX = "dly_";

    public static final String DRC_TRANSACTION_TABLE_NAME = "gtid_executed";

    public static final String DRC_DB_DELAY_MONITOR_TABLE_NAME_PREFIX = "dly_";

    public static final String DRC_DB_TRANSACTION_TABLE_NAME_PREFIX = "tx_";

    public static final String DRC_TRANSACTION_TABLE_PREFIX = "tx_";

    public static final String DRC_WRITE_FILTER_TABLE_NAME = "drc_write_filter";

    public static final String DRC_DELAY_MONITOR_NAME = DRC_MONITOR_SCHEMA_NAME + "." + DRC_DELAY_MONITOR_TABLE_NAME;

    public static final String DRC_DELAY_MONITOR_NAME_REGEX = DRC_MONITOR_SCHEMA_NAME + "\\." + DRC_DELAY_MONITOR_TABLE_NAME;

    public static final String MESSENGER_DELAY_MONITOR_TOPIC = "bbz.drc.delaymonitor";

    public static final String DRC_MQ = "_drc_mq";

    public static final String EVENT_LOG = "com.ctrip.framework.drc.replicator.impl.inbound.filter.TransactionMonitorFilter";

    public static final Logger EVENT_LOGGER = LoggerFactory.getLogger(EVENT_LOG);

    public static final String GTID_LOG = "com.ctrip.framework.drc.replicator.impl.inbound.event.ReplicatorLogEventHandler";

    public static final Logger GTID_LOGGER = LoggerFactory.getLogger(GTID_LOG);

    public static final String DELAY_LOG = "com.ctrip.framework.drc.replicator.impl.oubound.handler.DelayMonitorCommandHandler";

    public static final Logger DELAY_LOGGER = LoggerFactory.getLogger(DELAY_LOG);

    public static final String STATE_CONTROLLER_LOG = "com.ctrip.framework.drc.manager.ha.cluster.impl.DefaultInstanceStateController";

    public static final Logger STATE_LOGGER = LoggerFactory.getLogger(STATE_CONTROLLER_LOG);

    public static final String DDL_LOG = "com.ctrip.framework.drc.replicator.impl.inbound.filter.DdlFilter";

    public static final Logger DDL_LOGGER = LoggerFactory.getLogger(DDL_LOG);

    public static final String HEARTBEAT_LOG = "HEARTBEAT";

    public static final Logger HEARTBEAT_LOGGER = LoggerFactory.getLogger(HEARTBEAT_LOG);

    public static final String META_LOG = "metaLogger";

    public static final Logger META_LOGGER = LoggerFactory.getLogger(META_LOG);

    public static final String CONSOLE_DELAY_MONITOR_LOG = "delayMonitorLogger";

    public static final Logger CONSOLE_DELAY_MONITOR_LOGGER = LoggerFactory.getLogger(CONSOLE_DELAY_MONITOR_LOG);
    public static final String CONSOLE_DB_DELAY_MONITOR_LOG = "dbDelayMonitorLogger";

    public static final Logger CONSOLE_DB_DELAY_MONITOR_LOGGER = LoggerFactory.getLogger(CONSOLE_DB_DELAY_MONITOR_LOG);
    public static final String CONSOLE_BEACON_HEALTH_LOG = "beaconHealthLogger";

    public static final Logger CONSOLE_BEACON_HEALTH_LOGGER = LoggerFactory.getLogger(CONSOLE_BEACON_HEALTH_LOG);

    public static final String CONSOLE_DATA_CONSISTENCY_MONITOR_LOG = "consistencyMonitorLogger";

    public static final Logger CONSOLE_DC_LOGGER = LoggerFactory.getLogger(CONSOLE_DATA_CONSISTENCY_MONITOR_LOG);

    public static final String CONSOLE_GTID_LOG = "gtidMonitorLogger";

    public static final Logger CONSOLE_GTID_LOGGER = LoggerFactory.getLogger(CONSOLE_GTID_LOG);

    public static final String CONSOLE_AUTO_INCREMENT_LOG = "autoIncrementIdMonitorLogger";

    public static final Logger CONSOLE_AUTO_INCREMENT_LOGGER = LoggerFactory.getLogger(CONSOLE_AUTO_INCREMENT_LOG);

    public static final String CONSOLE_MYSQL_LOG = "mySqlMonitorLogger";

    public static final Logger CONSOLE_MYSQL_LOGGER = LoggerFactory.getLogger(CONSOLE_MYSQL_LOG);

    public static final String CONSOLE_TABLE_LOG = "tableConsistencyMonitorLogger";

    public static final Logger CONSOLE_TABLE_LOGGER = LoggerFactory.getLogger(CONSOLE_TABLE_LOG);

    public static final String NOTIFY_LOG = "com.ctrip.framework.drc.manager.healthcheck.notifier";

    public static final Logger NOTIFY_LOGGER = LoggerFactory.getLogger(NOTIFY_LOG);

    public static final Logger ROWS_FILTER_LOGGER = LoggerFactory.getLogger("ROWS FILTER");

    //test 相关
    public static final String MYSQL_USER_NAME = "root";

    public static final String EMBEDDED_MYSQL_PASSWORD = "root";

    public static final String MYSQL_PASSWORD = EMBEDDED_MYSQL_PASSWORD;

    public static final String INTEGRITY_TEST = "unit_test";

    public static final String MHA_NAME_TEST = "mha_name_test";

    public static final String DDL_SWITCH = "drc.ddl.switch";

    public static final String DAL_SWITCH = "drc.dal.switch";

    public static final String BENCHMARK_SWITCH_TEST = "drc.test.benchmark.switch";

    public static final String REVERSE_REPLICATOR_SWITCH_TEST = "drc.reverse.replicator.switch";

    public static final String MYSQL_LOCAL_INSTANCE_TEST = "drc.mysql.local";

    public static final String MYSQL_DB_INIT_TEST = "drc.monitor.mysql.initdb";

    public static final String INTEGRITY_TEST_QPS = "drc.monitor.qps.round";

    public static final String INTEGRITY_TEST_AUTO_WRITE = "drc.monitor.auto.write";

    public static final int EMPTY_DRC_UUID_EVENT_SIZE = 21;

    public static final int EMPTY_PREVIOUS_GTID_EVENT_SIZE = 31;

    public static final String MAX_BATCH_EXECUTE_SIZE = "max.batch.execute.size";

    public static final String TRANSACTION_TABLE_SIZE = "transaction.table.size";

    public static final String DEFAULT_TRANSACTION_TABLE_SIZE = "50000";

    public static final String TRANSACTION_TABLE_MERGE_SIZE = "transaction.table.merge.size";

    public static final String DEFAULT_TRANSACTION_TABLE_MERGE_SIZE = "10000";

    public static final String DEFAULT_CONFIG_FILE_NAME = "drc.properties";

    public static final int PROCESSORS_SIZE = OsUtils.getCpuCount();

    public static final String TIME_SPAN_KEY = "heartbeat.valid.time";

    public static final long TIME_SPAN_MS = Long.parseLong(System.getProperty(TIME_SPAN_KEY, "1000"));

    public static boolean isIntegrityTest() {
        return "true".equalsIgnoreCase(System.getProperty(REPLICATOR_WHITE_LIST));
    }

}
