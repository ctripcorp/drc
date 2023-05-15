package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaMappingTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/4/23 20:05
 */
@Repository
public class RowsFilterMetaMappingTblDao extends AbstractDao<RowsFilterMetaMappingTbl> {

    private static final String META_FILTER_ID = "meta_filter_id";
    private static final String DELETED = "deleted";
    private static final String FILTER_KEY = "filter_key";

    public RowsFilterMetaMappingTblDao() throws SQLException {
        super(RowsFilterMetaMappingTbl.class);
    }

    public List<RowsFilterMetaMappingTbl> queryByMetaFilterId(Long metaFilterId) throws SQLException {
        if (metaFilterId == null || metaFilterId == 0L) {
            return new ArrayList<>();
        }
        return queryByMetaFilterIdS(Collections.singletonList(metaFilterId));
    }

    public List<RowsFilterMetaMappingTbl> queryByMetaFilterIdS(List<Long> metaFilterIds) throws SQLException {
        if (CollectionUtils.isEmpty(metaFilterIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(META_FILTER_ID, metaFilterIds, Types.BIGINT).and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<RowsFilterMetaMappingTbl> queryByFilterKey(String filterKey) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().like(FILTER_KEY, "%" + filterKey + "%", Types.VARCHAR);
        return client.query(sqlBuilder, new DalHints());
    }
}
