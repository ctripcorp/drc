package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.RegionTbl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/25 12:05
 */
@Repository
public class RegionTblDao extends AbstractDao<RegionTbl> {

    public RegionTblDao() throws SQLException {
        super(RegionTbl.class);
    }

    @Override
    public List<RegionTbl> queryAll() throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.selectAll().orderBy("id", true);
        return client.query(sqlBuilder, new DalHints());
    }
}
