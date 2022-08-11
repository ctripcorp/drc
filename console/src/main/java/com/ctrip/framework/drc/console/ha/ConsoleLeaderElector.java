package com.ctrip.framework.drc.console.ha;

import com.ctrip.framework.drc.console.utils.SpringUtils;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.xpipe.cluster.AbstractLeaderElector;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @ClassName ConsoleLeaderElector
 * @Author haodongPan
 * @Date 2022/6/24 16:39
 * @Version: $
 */
// About Order:
// 1 -> important monitor or schedule task
// 2 -> other monitor or task 
// 3 -> leaderElector
@Component
@Order(3)
//@DependsOn({
//        "springUtils", "periodicalUpdateDbTask", "listenReplicatorTask", "consistentMonitorContainer",
//        "gtidMonitorTask", "checkTableConsistencyTask", "checkIncrementIdTask", "uuidMonitor",
//        "btdhsMonitor", "ddlMonitor", "periodicalRegisterBeaconTask", "syncMhaTask", "syncTableConfigTask", 
//        "updateDataConsistencyMetaTask", "clearConflictLog"
//})
@DependsOn({"springUtils"})
public class ConsoleLeaderElector extends AbstractLeaderElector {
    
    public static final String CONSOLE_LEADER_ELECTOR_PATH = "/console/leader";
    
    @PostConstruct
    public void leaderElectorInit(){
        try {
            this.setApplicationContext(SpringUtils.getApplicationContext());
            this.initialize();
            this.start();
        } catch (Exception e) {
            logger.error("[[component=leaderElector]] leader elector start error",e);
        }
    }

    @Override
    protected String getServerId() {
        return SystemConfig.LOCAL_SERVER_ADDRESS;
    }

    @Override
    protected String getLeaderElectPath() {
        return CONSOLE_LEADER_ELECTOR_PATH;
    }
    
}
