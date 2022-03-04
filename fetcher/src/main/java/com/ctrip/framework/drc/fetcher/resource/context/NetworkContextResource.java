package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.fetcher.system.qconfig.ConfigKey;
import org.apache.commons.lang3.StringUtils;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DEFAULT_CONFIG_FILE_NAME;

/**
 * @Author Slight
 * Dec 20, 2019
 */
public class NetworkContextResource extends AbstractContext implements EventGroupContext {

    @InstanceConfig(path="gtidExecuted")
    public String initialGtidExecuted;

    @InstanceConfig(path = "registryKey")
    public String registryKey;

    @Override
    public void doInitialize() throws Exception {
        super.doInitialize();
        GtidSet executedGtidSet = new GtidSet(initialGtidExecuted);
        String initialGtidPurged = (String) source.get(ConfigKey.from(DEFAULT_CONFIG_FILE_NAME, registryKey + ".purgedgtid"));
        if (StringUtils.isNotBlank(initialGtidExecuted) && initialGtidPurged != null) {
            GtidSet purgedGtidSet = new GtidSet(initialGtidPurged);
            executedGtidSet = executedGtidSet.union(purgedGtidSet);
            logger.info("[Union] executedGtidSet {} and initialGtidPurged {}", initialGtidExecuted, initialGtidPurged);
        }
        updateGtidSet(executedGtidSet);
        updateGtid("");
        logger.info("NETWORK GTID EXECUTED: {}, PURGED: {}", initialGtidExecuted, initialGtidPurged);
    }

}
