package com.ctrip.framework.drc.console.ha;

import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.xpipe.cluster.AbstractLeaderElector;
import org.springframework.stereotype.Component;

/**
 * @ClassName ConsoleLeaderElector
 * @Author haodongPan
 * @Date 2022/6/24 16:39
 * @Version: $
 */
@Component
public class ConsoleLeaderElector extends AbstractLeaderElector {
    
    public static String CONSOLE_LEADER_ELECTOR_PATH = "/console/leader";
    
    @Override
    protected String getServerId() {
        return SystemConfig.LOCAL_SERVER_ADDRESS;
    }

    @Override
    protected String getLeaderElectPath() {
        return CONSOLE_LEADER_ELECTOR_PATH;
    }
    
}
