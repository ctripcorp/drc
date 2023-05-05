package com.ctrip.framework.drc.core.server.common;

import com.ctrip.framework.drc.core.driver.config.InstanceStatus;
import com.ctrip.framework.drc.core.server.container.ZookeeperValue;
import com.ctrip.framework.drc.core.server.utils.IpUtils;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.server.zookeeper.DrcZkConfig;
import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.cluster.ElectContext;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Author limingdong
 * @create 2020/9/16
 */
public abstract class AbstractResourceManager implements Releasable {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected Map<String, LeaderElector> zkLeaderElectors = Maps.newConcurrentMap();

    private final static int RESTART_DELAY_TIME = 5;

    private ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(getClass().getSimpleName());

    @Autowired
    private DrcZkConfig drcZkConfig;

    @Autowired
    private LeaderElectorManager leaderElectorManager;

    @Value("${server.port}")
    public String port = "8080";

    public synchronized LeaderElector getLeaderElector(String registryKey) {
        LeaderElector leaderElector = zkLeaderElectors.get(registryKey);
        if (leaderElector == null) {
            String serverPath = drcZkConfig.getRegisterPath() + "/" + registryKey;
            ZookeeperValue zookeeperValue = new ZookeeperValue(IpUtils.getFistNonLocalIpv4ServerAddress(), Integer.parseInt(port), registryKey, InstanceStatus.INACTIVE.getStatus());
            String leaderElectionID = Codec.DEFAULT.encode(zookeeperValue);
            ElectContext ctx = new ElectContext(serverPath, leaderElectionID);
            leaderElector = leaderElectorManager.createLeaderElector(ctx);
            zkLeaderElectors.put(registryKey, leaderElector);
            try {
                leaderElector.initialize();
                leaderElector.start();
            } catch (Exception e) {
                logger.error("LeaderElector error for {}, lifecycle state is {}", registryKey, leaderElector.getLifecycleState(), e);
                scheduleRestart(leaderElector, registryKey);
            }
        }
        return leaderElector;
    }

    private void scheduleRestart(LeaderElector leaderElector, String registryKey) {
        if (leaderElector.getLifecycleState().isInitialized()) {
            scheduledExecutorService.schedule(() -> doRestart(leaderElector, registryKey), RESTART_DELAY_TIME, TimeUnit.SECONDS);
        }
    }

    private void doRestart(LeaderElector leaderElector, String registryKey) {
        try {
            if (leaderElector.getLifecycleState().isInitialized()) {
                logger.info("LeaderElector restart for {}", registryKey);
                leaderElector.start();
                if (leaderElector != zkLeaderElectors.get(registryKey)) {
                    leaderElector.stop();
                    leaderElector.dispose();
                }
            }
        } catch (Exception e) {
            logger.error("LeaderElector error when restart for {}", registryKey, e);
            scheduleRestart(leaderElector, registryKey);
        }
    }

    @Override
    public void release() {
        logger.info("run AbstractResourceManager resources release");
        scheduledExecutorService.shutdown();
        for (Lifecycle lifecycle : zkLeaderElectors.values()) {
            try {
                lifecycle.stop();
                lifecycle.dispose();
            } catch (Exception e) {
                logger.error("lifecycle stop error", e);
            }
        }
    }
}
