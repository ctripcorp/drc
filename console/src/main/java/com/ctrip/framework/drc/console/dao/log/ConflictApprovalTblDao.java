package com.ctrip.framework.drc.console.dao.log;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictApprovalTbl;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictAutoHandleBatchTbl;
import com.ctrip.framework.drc.console.param.log.ConflictApprovalQueryParam;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/10/30 20:17
 */
@Repository
public class ConflictApprovalTblDao extends AbstractDao<ConflictApprovalTbl> {

    private static final String DB_NAME = "db_name";
    private static final String TABLE_NAME = "table_name";
    private static final String APPLICANT = "applicant";
    private static final String APPROVAL_RESULT = "approval_result";
    private static final String DATACHANGE_LASTTIME = "datachange_lasttime";
    private static final String BATCH_ID = "batch_id";

    @Autowired
    private ConflictAutoHandleBatchTblDao conflictAutoHandleBatchTblDao;

    public ConflictApprovalTblDao() throws SQLException {
        super(ConflictApprovalTbl.class);
    }

    public List<ConflictApprovalTbl> queryByParam(ConflictApprovalQueryParam param) throws SQLException {
        SelectSqlBuilder sqlBuilder = buildSqlBuilder(param);
        sqlBuilder.selectCount();
        int count = client.count(sqlBuilder, new DalHints()).intValue();
        param.getPageReq().setTotalCount(count);

        sqlBuilder = buildSqlBuilder(param);
        sqlBuilder.selectAll().atPage(param.getPageReq().getPageIndex(), param.getPageReq().getPageSize()).orderBy(DATACHANGE_LASTTIME, false);
        return queryList(sqlBuilder);
    }

    public ConflictApprovalTbl queryByBatchId(long batchId) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(BATCH_ID, batchId, Types.BIGINT);
        return queryOne(sqlBuilder);
    }

    private SelectSqlBuilder buildSqlBuilder(ConflictApprovalQueryParam param) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();

        if (StringUtils.isNotBlank(param.getDbName()) || StringUtils.isNotBlank(param.getTableName())) {
            List<ConflictAutoHandleBatchTbl> batchTbls = conflictAutoHandleBatchTblDao.queryByDb(param.getDbName(), param.getTableName());
            List<Long> batchIds = batchTbls.stream().map(ConflictAutoHandleBatchTbl::getId).collect(Collectors.toList());
            sqlBuilder.and().in(BATCH_ID, batchIds, Types.BIGINT);
        }

        sqlBuilder.and().equalNullable(APPLICANT, param.getApplicant(), Types.VARCHAR)
                .and().equalNullable(APPROVAL_RESULT, param.getApprovalResult(), Types.TINYINT);

        return sqlBuilder;
    }
}
