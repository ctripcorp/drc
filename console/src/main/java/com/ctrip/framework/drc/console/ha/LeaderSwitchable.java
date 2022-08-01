package com.ctrip.framework.drc.console.ha;


import com.ctrip.xpipe.api.cluster.LeaderAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface LeaderSwitchable extends LeaderAware {
    
    void doSwitchToStart() throws Throwable;
    
    void doSwitchToStop() throws Throwable;
    
    default void switchToStart() {
        Logger logger = LoggerFactory.getLogger(this.getClass());
        LeaderSwitchUtil.getWorkers().submit(
                () -> {
                    try {
                        logger.info("[[tag=leaderSwitch]] {} switchToStart",this.getClass().getSimpleName());
                        this.doSwitchToStart();
                    } catch (Throwable t) {
                        logger.info("switch to start error");
                    }
                }
        );
    }
    
    default void switchToStop() {
        Logger logger = LoggerFactory.getLogger(this.getClass());
        LeaderSwitchUtil.getWorkers().submit(
                () -> {
                    try {
                        logger.info("[[tag=leaderSwitch]] {} switchToStop",this.getClass().getSimpleName());
                        this.doSwitchToStop();
                    } catch (Throwable t) {
                        logger.info("switch to stop error");
                    }
                } 
        );
    }
}
