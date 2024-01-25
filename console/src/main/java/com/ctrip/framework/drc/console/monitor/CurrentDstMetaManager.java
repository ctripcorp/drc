package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.pojo.MonitorMetaInfo;
import org.springframework.stereotype.Component;

import java.sql.SQLException;

/**
 * manage mha info whose <b>src mha is monitored</b>
 * @author yongnian
 */
@Component
public class CurrentDstMetaManager extends DefaultCurrentMetaManager {
    @Override
    protected MonitorMetaInfo getMonitorMetaInfo() throws SQLException {
        return cacheMetaService.getDstMonitorMetaInfo();
    }
}
