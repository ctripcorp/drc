package com.ctrip.framework.drc.applier.mq;

import com.ctrip.framework.drc.applier.container.SpringContextHolder;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.SpringZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.springframework.context.ApplicationContext;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.REPLICATOR_UUIDS_PATH;

/**
 * Created by jixinwang on 2022/10/19
 */
public class MqPositionResource extends AbstractResource {

    @InstanceConfig(path="gtidExecuted")
    public String initialGtidExecuted = "";

    private GtidSet executedGtidSet;

    private ZkClient zkClient;

    public ScheduledExecutorService timer = ThreadUtils.newSingleThreadScheduledExecutor("Update-Position-Task");

    @Override
    protected void doInitialize() throws Exception {
        executedGtidSet = new GtidSet(initialGtidExecuted);
        initZkClient();
    }

    @Override
    public void doStart() {
        timer.scheduleWithFixedDelay(() -> {
            setGtidSet("", executedGtidSet.toString());
        }, 3, 10, TimeUnit.SECONDS);
    }

    public synchronized void update(String gtid) {
        executedGtidSet.add(gtid);
    }

    public void setGtidSet(String key, String gtidSet) {
        CuratorFramework curatorFramework = zkClient.get();
        try {
            String registerPath = REPLICATOR_UUIDS_PATH + "/" + key;
            checkPath(curatorFramework, registerPath);
            curatorFramework.inTransaction().check().forPath(registerPath).and().setData().forPath(registerPath, Codec.DEFAULT.encodeAsBytes(gtidSet)).and().commit();
        } catch (Exception e) {
            logger.error("setUuids error for {}", key, e);
        }
    }

    private boolean checkPath(CuratorFramework curatorFramework, String registerPath) throws Exception {
        if (curatorFramework.checkExists().forPath(registerPath) == null) {
            curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(registerPath);
            return false;
        }
        return true;
    }


    private void initZkClient() {
        try {
            ApplicationContext applicationContext = SpringContextHolder.getApplicationContext();
            if (applicationContext != null) {
                zkClient = applicationContext.getBean(SpringZkClient.class);
            }
        } catch (Exception e) {
            logger.error("initMetaManager error", e);
        }
    }
}
