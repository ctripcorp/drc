package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.ha.LeaderSwitchable;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;

import java.util.concurrent.ExecutorService;

/**
 * @ClassName AbstractSwitchableMonitor
 * @Author haodongPan
 * @Date 2022/8/3 19:35
 * @Version: $
 */
public abstract class AbstractLeaderAwareMonitor extends AbstractMonitor implements LeaderSwitchable {

    public static final ExecutorService leaderSwitchWorkers = ThreadUtils.newCachedThreadPool("leader-switch-worker");

    protected volatile boolean isRegionLeader = false;

    @Override
    public void switchToLeader() throws Throwable {
        // default
        this.scheduledTask();
    }

    @Override
    public void switchToSlave() throws Throwable {
        // default
        this.scheduledTask();
    }

    @Override
    public void isleader() {
        isRegionLeader = true;
        leaderSwitchWorkers.submit(
                () -> {
                    try {
                        logger.info("[[tag=leaderSwitch]] {} switchToLeader", this.getClass().getSimpleName());
                        switchToLeader();
                    } catch (Throwable t) {
                        logger.warn("switch to leader error");
                    }
                }
        );
    }
    
    @Override
    public void notLeader() {
        isRegionLeader = false;
        leaderSwitchWorkers.submit(
                () -> {
                    try {
                        logger.info("[[tag=leaderSwitch]] {} switchToSlave", this.getClass().getSimpleName());
                        switchToSlave();
                    } catch (Throwable t) {
                        logger.warn("switch to leader error");
                    }
                }
        );
    }
    

}
