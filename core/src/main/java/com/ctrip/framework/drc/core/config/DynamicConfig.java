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

    private static final String DATASOURCE_SOCKET_TIMEOUT = "datasource.socket.timeout";

    private static final String TABLE_PARTITION_SWITCH = "table.partition.switch";

    private static final String INDEPENDENT_EMBEDDED_MYSQL_SWITCH = "independent.embedded.mysql.switch";

    private static final String INDEPENDENT_EMBEDDED_MYSQL_SWITCH_KEY = INDEPENDENT_EMBEDDED_MYSQL_SWITCH + ".%s";
    private static final String UPGRADE_EMBEDDED_MYSQL_SWITCH = "upgrade.8.embedded.mysql.switch";
    private static final String UPGRADE_EMBEDDED_MYSQL_SWITCH_KEY = UPGRADE_EMBEDDED_MYSQL_SWITCH + ".%s";

    private static final String PURGED_GTID_SET_CHECK_SWITCH = "purged.gtid.set.check.switch";

    private static final String RECEIVE_CHECK_SWITCH = "receive.check.switch";

    private DynamicConfig() {}

    private static class ConfigHolder {
        public static final DynamicConfig INSTANCE = new DynamicConfig();
    }

    public static DynamicConfig getInstance() {
        return ConfigHolder.INSTANCE;
    }

    public int getConcurrency(String key) {
        return getIntProperty(String.format(CONCURRENCY, key), MAX_ACTIVE);
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


    public boolean getEmbeddedMySQLUpgradeTo8Switch(String key) {
        String value = getProperty(String.format(UPGRADE_EMBEDDED_MYSQL_SWITCH_KEY, key));
        if (StringUtils.isBlank(value)) {
            return getBooleanProperty(UPGRADE_EMBEDDED_MYSQL_SWITCH, false);
        }
        return Boolean.parseBoolean(value);
    }
}
