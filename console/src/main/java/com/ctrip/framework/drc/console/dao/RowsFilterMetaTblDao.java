package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaTbl;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by dengquanliang
 * 2023/4/23 19:47
 */
@Repository
public class RowsFilterMetaTblDao extends AbstractDao<RowsFilterMetaTbl> {

    private static final String META_FILTER_NAME = "meta_filter_name";

    public RowsFilterMetaTblDao(Class<RowsFilterMetaTbl> clazz) throws SQLException {
        super(clazz);
    }

    public RowsFilterMetaTbl queryByMetaFilterName(String metaFilterName) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(META_FILTER_NAME, metaFilterName, Types.VARCHAR);
        return client.queryFirst(sqlBuilder, null);
    }

}
