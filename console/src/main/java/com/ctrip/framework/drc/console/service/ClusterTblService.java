package com.ctrip.framework.drc.console.service;

import java.sql.SQLException;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-09
 */
public interface ClusterTblService {

    Integer getRecordsCount() throws SQLException;
}
