package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaMappingTbl;
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

    public RowsFilterMetaMappingTblDao(Class<RowsFilterMetaMappingTbl> clazz) throws SQLException {
        super(clazz);
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
        sqlBuilder.selectAll().in(META_FILTER_ID, metaFilterIds, Types.BIGINT);
        return client.query(sqlBuilder, null);
    }
}
