package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.DataInconsistencyHistoryTbl;

import java.sql.SQLException;

/**
 * Created by jixinwang on 2021/2/21
 */
public class DataInconsistencyHistoryTblDao extends AbstractDao<DataInconsistencyHistoryTbl> {
    public DataInconsistencyHistoryTblDao() throws SQLException {
        super(DataInconsistencyHistoryTbl.class);
    }
}
