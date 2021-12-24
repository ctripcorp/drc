package com.ctrip.framework.drc.core.server.common;

import com.ctrip.framework.drc.core.driver.config.InstanceStatus;
import com.ctrip.framework.drc.core.server.container.ZookeeperValue;
import com.ctrip.framework.drc.core.server.utils.IpUtils;
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

/**
 * @Author limingdong
 * @create 2020/9/16
 */
public abstract class AbstractResourceManager implements Releasable {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected Map<String, LeaderElector> zkLeaderElectors = Maps.newConcurrentMap();

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
                logger.error("LeaderElector error for {}", registryKey, e);
            }
        }
        return leaderElector;
    }

    @Override
    public void release() {
        logger.info("run AbstractResourceManager resources release");
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
