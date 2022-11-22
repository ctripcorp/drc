package com.ctrip.framework.drc.console.service.monitor;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by jixinwang on 2021/8/2
 */
public interface MonitorService {

    @Deprecated void switchMonitors(List<Long> mhaGroupIds, String status) throws SQLException;
    
    void switchMonitors(String mhaName, String status) throws SQLException;

    List<String> queryMhaNamesToBeMonitored() throws SQLException;

    @Deprecated List<Long> queryMhaIdsToBeMonitored() throws SQLException;

    List<String> getMhaNamesToBeMonitored() throws SQLException;
}
