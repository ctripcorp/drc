package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.platform.dal.dao.*;
import com.ctrip.platform.dal.dao.sqlbuilder.*;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;

import com.ctrip.platform.dal.dao.helper.DalDefaultJpaParser;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

/**
 * @author phd潘昊栋
 * @date 2022-04-28
 */
@Repository
public class RowsFilterTblDao extends AbstractDao<RowsFilterTbl>{

    public RowsFilterTblDao() throws SQLException {
        super(RowsFilterTbl.class);
    }

    public RowsFilterTbl queryById(Long rowsFilterId, Integer deleted) throws SQLException {
        List<RowsFilterTbl> rowsFilterTbls = queryByIds(Lists.newArrayList(rowsFilterId), deleted);
        if (CollectionUtils.isEmpty(rowsFilterTbls) || rowsFilterTbls.size() != 1) {
            throw new SQLException("sql result error in queryRowsFilterTblById: " + rowsFilterTbls);
        }
        return rowsFilterTbls.get(0);
    }
    
    public List<RowsFilterTbl> queryByIds(List<Long> rowsFilterIds, Integer deleted) throws SQLException {
        if(CollectionUtils.isEmpty(rowsFilterIds)) {
            throw new IllegalArgumentException("build sql: query RowsFilterTbl By Ids ,but rowsFilterIds is empty");
        }
        SelectSqlBuilder builder = new SelectSqlBuilder();
        builder.selectAll().in("id", rowsFilterIds, Types.BIGINT)
                .and().equal("deleted", deleted, Types.TINYINT);
        return client.query(builder,new DalHints());
    }
}