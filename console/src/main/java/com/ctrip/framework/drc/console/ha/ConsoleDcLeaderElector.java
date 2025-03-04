package com.ctrip.framework.drc.console.ha;

import com.ctrip.framework.drc.console.monitor.delay.config.DataCenterService;
import com.ctrip.framework.drc.core.server.AbstractDcLeaderElector;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.utils.SpringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created by dengquanliang
 * 2025/2/13 14:45
 */
@Component
@Order(3)
@DependsOn({"springUtils"})
public class ConsoleDcLeaderElector extends AbstractDcLeaderElector {


    @Autowired
    private DataCenterService dataCenterService;

    public static final String CONSOLE_DC_LEADER_ELECTOR_PATH = "/console/dc/%s/leader";

    @PostConstruct
    public void leaderElectorInit(){
        try {
            this.setApplicationContext(SpringUtils.getApplicationContext());
            this.initialize();
            this.start();
        } catch (Exception e) {
            logger.error("[[component=leaderElector]] ConsoleDcLeaderElector elector start error",e);
        }
    }

    @Override
    protected String getServerId() {
        return SystemConfig.LOCAL_SERVER_ADDRESS;
    }

    @Override
    protected String getLeaderElectPath() {
        return String.format(CONSOLE_DC_LEADER_ELECTOR_PATH, dataCenterService.getDc());
    }
}
