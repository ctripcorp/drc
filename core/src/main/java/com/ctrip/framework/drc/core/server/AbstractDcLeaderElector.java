package com.ctrip.framework.drc.core.server;

import com.ctrip.xpipe.cluster.AbstractLeaderElector;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by dengquanliang
 * 2025/2/13 14:24
 */
public abstract class AbstractDcLeaderElector extends AbstractLeaderElector {

    @Autowired
    private ZkClient zkClient;

    private ExecutorService executors = Executors.newCachedThreadPool(XpipeThreadFactory.create(getClass().getSimpleName()));

    private ApplicationContext applicationContext;

    private LeaderLatch leaderLatch;

    @Override
    protected void doStart() throws Exception {

        leaderLatch = new LeaderLatch(zkClient.get(), getLeaderElectPath(), getServerId());
        leaderLatch.addListener(new LeaderLatchListener() {

            @Override
            public void isLeader() {

                logger.info("[isLeader]({})", getServerId());
                Map<String, DcLeaderAware> leaderawares = applicationContext.getBeansOfType(DcLeaderAware.class);
                for (Map.Entry<String, DcLeaderAware> entry : leaderawares.entrySet()) {
                    try{
                        logger.info("[isLeader][notify]{}", entry.getKey());
                        entry.getValue().isleader();
                    }catch (Exception e){
                        logger.error("[isLeader]" + entry, e);
                    }
                }
            }

            @Override
            public void notLeader() {

                logger.info("[notLeader]{}", getServerId());
                Map<String, DcLeaderAware> leaderawares = applicationContext.getBeansOfType(DcLeaderAware.class);
                for (Map.Entry<String, DcLeaderAware> entry : leaderawares.entrySet()) {
                    try{
                        logger.info("[notLeader][notify]{}", entry.getKey());
                        entry.getValue().notLeader();
                    }catch (Exception e){
                        logger.error("[notLeader]" + entry, e);
                    }
                }
            }
        }, executors);
        leaderLatch.start();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
