package com.ctrip.framework.drc.core.server.ha.zookeeper;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.cluster.ElectContext;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

/**
 * @Author limingdong
 * @create 2020/4/1
 */
public class DrcLeaderElector extends AbstractLifecycle implements LeaderElector {

    private LeaderLatch latch;

    private ElectContext ctx;

    private CuratorFramework zkClient;

    public DrcLeaderElector(ElectContext ctx, CuratorFramework zkClient) {
        this.ctx = ctx;
        this.zkClient = zkClient;
    }

    @Override
    protected void doStart() throws Exception {
        elect();
    }

    @Override
    public void elect() throws Exception {

        zkClient.createContainers(ctx.getLeaderElectionZKPath());

        latch = new LeaderLatch(zkClient, ctx.getLeaderElectionZKPath(), ctx.getLeaderElectionID());
        latch.addListener(new LeaderLatchListener() {

            @Override
            public void notLeader() {
                logger.info("{} not leader", ctx.getLeaderElectionID());
            }

            @Override
            public void isLeader() {
                logger.info("{} is leader", ctx.getLeaderElectionID());

            }
        });

        latch.start();
        logger.info("[elect]{}", ctx);
    }

    @Override
    public void doStop() throws Exception {
        if (latch != null) {
            latch.close();
        }
    }

}
