package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplicationFormTbl;
import com.ctrip.framework.drc.console.param.v2.application.ApplicationFormQueryParam;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.MatchPattern;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Created by dengquanliang
 * 2024/1/31 16:39
 */
@Repository
public class ApplicationFormTblDao extends AbstractDao<ApplicationFormTbl> {

    private static final String DB_NAME = "db_name";
    private static final String TABLE_NAME = "table_name";
    private static final String SRC_REGION = "src_region";
    private static final String DST_REGION = "dst_region";
    private static final String REPLICATION_TYPE = "replication_type";
    private static final String FILTER_TYPE = "filter_type";
    private static final String DATACHANGE_LASTTIME = "datachange_lasttime";

    public ApplicationFormTblDao() throws SQLException {
        super(ApplicationFormTbl.class);
    }

    public List<ApplicationFormTbl> queryByParam(ApplicationFormQueryParam param) throws SQLException {
        SelectSqlBuilder sqlBuilder = buildSqlBuilder(param);
        sqlBuilder.selectCount();
        int count = client.count(sqlBuilder, new DalHints()).intValue();
        param.getPageReq().setTotalCount(count);

        sqlBuilder = buildSqlBuilder(param);
        sqlBuilder.selectAll().atPage(param.getPageReq().getPageIndex(), param.getPageReq().getPageSize()).orderBy(DATACHANGE_LASTTIME, false);
        return queryList(sqlBuilder);
    }

    public SelectSqlBuilder buildSqlBuilder(ApplicationFormQueryParam param) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        if (StringUtils.isNotBlank(param.getDbName())) {
            sqlBuilder.and().like(DB_NAME, param.getDbName(), MatchPattern.CONTAINS, Types.VARCHAR);
        }
        if (StringUtils.isNotBlank(param.getTableName())) {
            sqlBuilder.and().like(TABLE_NAME, param.getTableName(), MatchPattern.CONTAINS, Types.VARCHAR);
        }
        if (StringUtils.isNotBlank(param.getSrcRegion())) {
            sqlBuilder.and().equal(SRC_REGION, param.getSrcRegion(), Types.VARCHAR);
        }
        if (StringUtils.isNotBlank(param.getDstRegion())) {
            sqlBuilder.and().equal(DST_REGION, param.getDstRegion(), Types.VARCHAR);
        }
        if (param.getReplicationType() != null) {
            sqlBuilder.and().equal(REPLICATION_TYPE, param.getReplicationType(), Types.TINYINT);
        }
        if (StringUtils.isNotBlank(param.getFilterType())) {
            sqlBuilder.and().equal(FILTER_TYPE, param.getFilterType(), Types.VARCHAR);
        }

        return sqlBuilder;
    }


}
