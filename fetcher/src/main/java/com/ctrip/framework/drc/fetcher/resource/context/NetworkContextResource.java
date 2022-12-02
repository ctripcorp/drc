package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import com.ctrip.framework.drc.fetcher.system.qconfig.ConfigKey;
import org.apache.commons.lang3.StringUtils;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DEFAULT_CONFIG_FILE_NAME;

/**
 * @Author Slight
 * Dec 20, 2019
 */
public class NetworkContextResource extends AbstractContext implements EventGroupContext {

    @InstanceConfig(path = "gtidExecuted")
    public String initialGtidExecuted;

    @InstanceConfig(path = "registryKey")
    public String registryKey;

    @InstanceConfig(path = "applyMode")
    public int applyMode;

    @InstanceResource
    public MqPosition mqPosition;

    @Override
    public void doInitialize() throws Exception {
        super.doInitialize();
        GtidSet executedGtidSet = new GtidSet(initialGtidExecuted);
        String initialGtidPurged = (String) source.get(ConfigKey.from(DEFAULT_CONFIG_FILE_NAME, registryKey + ".purgedgtid"));
        logger.info("[{}][NETWORK GTID] executed from cluster manager: {}, from qconfig: {}", registryKey, initialGtidExecuted, initialGtidPurged);

        if (StringUtils.isNotBlank(initialGtidExecuted) && initialGtidPurged != null) {
            GtidSet purgedGtidSet = new GtidSet(initialGtidPurged);
            executedGtidSet = executedGtidSet.union(purgedGtidSet);
            logger.info("[{}][NETWORK GTID] union gtid executed", registryKey);
        }

        if (ApplyMode.mq == ApplyMode.getApplyMode(applyMode)) {
            String position = mqPosition.getPosition();
            executedGtidSet = executedGtidSet.union(new GtidSet(position));
        }

        logger.info("[{}][NETWORK GTID] executed gtidset: {}", registryKey, executedGtidSet);
        updateGtidSet(executedGtidSet);
        updateGtid("");
    }

}
