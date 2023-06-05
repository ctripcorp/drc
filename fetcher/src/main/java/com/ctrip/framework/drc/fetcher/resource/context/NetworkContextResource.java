package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.healthcheck.task.ExecutedGtidQueryTask;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import com.ctrip.framework.drc.fetcher.system.qconfig.ConfigKey;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.commons.lang3.StringUtils;

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
            return fetchGtidSet();
        }

        GtidSet executedGtidSet = unionGtidSet(initialGtidExecuted);
        updateGtidSet(executedGtidSet);
        return fetchGtidSet();
    }

    private GtidSet unionGtidSet(String initialGtidExecuted) {
        GtidSet executedGtidSet = new GtidSet(initialGtidExecuted);
        logger.info("[{}][NETWORK GTID] initial position: {}", registryKey, executedGtidSet);

        executedGtidSet = unionPositionFromQConfig(executedGtidSet);

        ConsumeType consumeType = ApplyMode.getApplyMode(applyMode).getConsumeType();
        switch (consumeType) {
            case Applier:
                executedGtidSet = unionPositionFromDb(executedGtidSet);
                break;
            case Messenger:
                executedGtidSet = unionPositionFromZk(executedGtidSet);
                break;
        }

        logger.info("[{}][NETWORK GTID] executed gtidset: {}", registryKey, executedGtidSet);
        return executedGtidSet;
    }

    private GtidSet unionPositionFromQConfig(GtidSet gtidSet) {
        String positionFromQConfig = (String) source.get(ConfigKey.from(DEFAULT_CONFIG_FILE_NAME, registryKey + ".purgedgtid"));
        if (StringUtils.isBlank(positionFromQConfig)) {
            return gtidSet;
        } else {
            logger.info("[{}][NETWORK GTID] qconfig position: {}", registryKey, positionFromQConfig);
            return gtidSet.union(new GtidSet(positionFromQConfig));
        }
    }

    private GtidSet unionPositionFromDb(GtidSet gtidSet) {
        GtidSet positionFromDb = queryPositionFromDb();
        logger.info("[{}][NETWORK GTID] db position: {}", registryKey, positionFromDb);
        return gtidSet.union(positionFromDb);
    }

    private GtidSet unionPositionFromZk(GtidSet gtidSet) {
        String positionFromZk = mqPosition.getPosition();
        if (StringUtils.isBlank(positionFromZk)) {
            return gtidSet;
        } else {
            logger.info("[{}][NETWORK GTID] zk position: {}", registryKey, positionFromZk);
            return gtidSet.union(new GtidSet(positionFromZk));
        }
    }

    private GtidSet queryPositionFromDb() {
        Endpoint endpoint = new DefaultEndPoint(ip, port, username, password);
        ExecutedGtidQueryTask queryTask = new ExecutedGtidQueryTask(endpoint);
        String gtidSet = queryTask.doQuery();
        return new GtidSet(gtidSet);
    }
}
