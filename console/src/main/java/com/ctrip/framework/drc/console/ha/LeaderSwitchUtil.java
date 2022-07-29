package com.ctrip.framework.drc.console.ha;

import com.ctrip.framework.drc.core.server.utils.ThreadUtils;

import java.util.concurrent.ExecutorService;

/**
 * @ClassName LeaderSwitcher
 * @Author haodongPan
 * @Date 2022/7/21 16:53
 * @Version: $
 */
public class LeaderSwitchUtil {
    
    private static final ExecutorService leaderSwitchWorkers = ThreadUtils.newCachedThreadPool("leader-switch-worker");
    
    public static ExecutorService getWorkers() {
        return leaderSwitchWorkers;
    }
    
}
