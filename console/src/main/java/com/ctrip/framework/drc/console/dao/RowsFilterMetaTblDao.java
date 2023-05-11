package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/4/23 19:47
 */
@Repository
public class RowsFilterMetaTblDao extends AbstractDao<RowsFilterMetaTbl> {

    private static final String ID = "id";
    private static final String META_FILTER_NAME = "meta_filter_name";
    private static final String DELETED = "deleted";

    public RowsFilterMetaTblDao() throws SQLException {
        super(RowsFilterMetaTbl.class);
    }

    public RowsFilterMetaTbl queryOneByMetaFilterName(String metaFilterName) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.BIGINT).and().equal(META_FILTER_NAME, metaFilterName, Types.VARCHAR);
        return client.queryFirst(sqlBuilder, new DalHints());
    }

    public RowsFilterMetaTbl queryById(Long id) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(ID, id, Types.BIGINT).and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return client.queryFirst(sqlBuilder, new DalHints());
    }

    public List<RowsFilterMetaTbl> queryByMetaFilterName(String metaFilterName) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.BIGINT);
        if (StringUtils.isNotBlank(metaFilterName)) {
            sqlBuilder.and().equal(META_FILTER_NAME, metaFilterName, Types.VARCHAR);
        }
        return client.query(sqlBuilder, new DalHints());
    }

    public List<RowsFilterMetaTbl> queryByIds(List<Long> ids, String metaFilterName) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(ID, ids, Types.BIGINT).and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        if (StringUtils.isNotBlank(metaFilterName)) {
            sqlBuilder.and().equal(META_FILTER_NAME, metaFilterName, Types.VARCHAR);
        }
        return client.query(sqlBuilder, new DalHints());
    }

}
