package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.DdlHistoryTbl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractTableSqlBuilder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * @author shb沈海波
 * @date 2021-01-04
 */
@Repository
public class DdlHistoryTblDao extends AbstractDao<DdlHistoryTbl> {
    public static final String CREATE_TIME = "create_time";
    
    public DdlHistoryTblDao() throws SQLException {
        super(DdlHistoryTbl.class);
    }

    public List<DdlHistoryTbl> queryByStartCreateTime(Timestamp startTime) throws SQLException {
        SelectSqlBuilder selectSqlBuilder = new SelectSqlBuilder();
        selectSqlBuilder.selectAll().greaterThan(CREATE_TIME, startTime,
                Types.TIMESTAMP);
        return client.query(selectSqlBuilder, new DalHints());
    }

    public List<DdlHistoryTbl> queryByDbAndTime(String db, String table, Timestamp startTime, Timestamp endTime) throws SQLException {
        SelectSqlBuilder selectSqlBuilder = new SelectSqlBuilder();
        selectSqlBuilder.selectAll()
                .equal("schema_name", db, Types.VARCHAR)
                .and().equal("table_name", table, Types.VARCHAR)
                .and().greaterThan(CREATE_TIME, startTime, Types.TIMESTAMP)
                .and().lessThan(CREATE_TIME, endTime, Types.TIMESTAMP);
        return client.query(selectSqlBuilder, new DalHints());
    }
}