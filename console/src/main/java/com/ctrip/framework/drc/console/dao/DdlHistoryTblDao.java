package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.DdlHistoryTbl;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * @author shb沈海波
 * @date 2021-01-04
 */
@Repository
public class DdlHistoryTblDao extends AbstractDao<DdlHistoryTbl> {
    public DdlHistoryTblDao() throws SQLException {
        super(DdlHistoryTbl.class);
    }
}