package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.healthcheck.task.ExecutedGtidQueryTask;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import com.ctrip.framework.drc.fetcher.system.qconfig.ConfigKey;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.commons.lang3.StringUtils;

import static com.ctrip.framework.drc.core.server.common.enums.ConsumeType.Applier;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DEFAULT_CONFIG_FILE_NAME;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.isIntegrityTest;

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

    @InstanceConfig(path = "target.ip")
    public String ip = "";

    @InstanceConfig(path = "target.port")
    public int port;

    @InstanceConfig(path = "target.username")
    public String username;

    @InstanceConfig(path = "target.password")
    public String password;

    @Override
    public void doInitialize() throws Exception {
        super.doInitialize();
        GtidSet executedGtidSet = unionGtidSet(initialGtidExecuted);
        updateGtidSet(executedGtidSet);
        updateGtid("");
    }

    public GtidSet queryTheNewestGtidset() {
        if (isIntegrityTest()) {
            return null;
        }

        if (Applier == ApplyMode.getApplyMode(applyMode).getConsumeType()) {
            Endpoint endpoint = new DefaultEndPoint(ip, port, username, password);
            ExecutedGtidQueryTask queryTask = new ExecutedGtidQueryTask(endpoint);
            String gtidFromDb = queryTask.doQuery();
            logger.info("[{}][NETWORK GTID] query the newest gtidset from DB is: {}", registryKey, gtidFromDb);
            if (StringUtils.isNotBlank(gtidFromDb)) {
                GtidSet executedGtidSet = unionGtidSet(gtidFromDb);
                updateGtidSet(executedGtidSet);
            }
        }

        return fetchGtidSet();
    }

    private GtidSet unionGtidSet(String initialGtidExecuted) {
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
        return executedGtidSet;
    }
}
