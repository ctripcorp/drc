package com.ctrip.framework.drc.console.ha;
import com.ctrip.xpipe.api.cluster.LeaderAware;

public interface LeaderSwitchable extends LeaderAware {
    
    
    void switchToLeader() throws Throwable;
    
    void switchToSlave() throws Throwable;
    
}
