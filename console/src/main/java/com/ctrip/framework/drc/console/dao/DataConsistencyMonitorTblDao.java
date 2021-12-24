package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.DataConsistencyMonitorTbl;

import java.sql.SQLException;

/**
 * Created by jixinwang on 2020/12/29
 */
public class DataConsistencyMonitorTblDao extends AbstractDao<DataConsistencyMonitorTbl> {
    public DataConsistencyMonitorTblDao() throws SQLException {
        super(DataConsistencyMonitorTbl.class);
    }
}
