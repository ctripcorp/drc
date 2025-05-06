package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.RouteTbl;
import com.ctrip.framework.drc.console.param.RouteQueryParam;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

@Repository
public class RouteTblDao extends AbstractDao<RouteTbl> {

    private static final String ROUTE_ORG_ID = "route_org_id";
    private static final String SRC_DC_ID = "src_dc_id";
    private static final String DST_DC_ID = "dst_dc_id";
    private static final String TAG = "tag";
    private static final String GLOBAL_ACTIVE = "global_active";
    private static final String DATACHANGE_LASTTIME = "datachange_lasttime";
    private static final String ID = "id";
    private static final boolean DESCENDING = false;


    public RouteTblDao() throws SQLException {
        super(RouteTbl.class);
    }

    public List<RouteTbl> queryByParam(RouteQueryParam param) throws SQLException {
        SelectSqlBuilder sqlBuilder = buildSqlBuilder(param);
        sqlBuilder.orderBy(ID, DESCENDING);
        return queryList(sqlBuilder);
    }

    private SelectSqlBuilder buildSqlBuilder(RouteQueryParam param) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        if (param.getBuId() > 0L) {
            sqlBuilder.and().equal(ROUTE_ORG_ID, param.getBuId(), Types.BIGINT);
        }
        if (StringUtils.isNotBlank(param.getTag())) {
            sqlBuilder.and().equal(TAG, param.getTag(), Types.VARCHAR);
        }
        if (param.getGlobalActive() != null) {
            sqlBuilder.and().equal(GLOBAL_ACTIVE, param.getGlobalActive(), Types.VARCHAR);
        }
        if (!CollectionUtils.isEmpty(param.getSrcDcIds())) {
            sqlBuilder.and().in(SRC_DC_ID, param.getSrcDcIds(), Types.BIGINT);
        }
        if (!CollectionUtils.isEmpty(param.getDstDcIds())) {
            sqlBuilder.and().in(DST_DC_ID, param.getDstDcIds(), Types.BIGINT);
        }
        return sqlBuilder;
    }
}